#include <Processors/Formats/Impl/Vortex/VortexColumnConverter.h>

#if USE_VORTEX

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <Core/Defines.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/Formats/IInputFormat.h>
#include <Processors/Formats/Impl/Vortex/VortexFFIHelpers.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <Common/assert_cast.h>

#include <base/scope_guard.h>

#include <arrow/type.h>

#include <vortex_ffi.h>

namespace ProfileEvents
{
extern const Event VortexDecodeMicroseconds;
extern const Event VortexConvertMicroseconds;
}

namespace DB
{
namespace ErrorCodes
{
extern const int INCORRECT_DATA;
extern const int LOGICAL_ERROR;
}
}

namespace DB::Vortex
{

namespace
{

/// The name the scan gives the column of row numbers it prepends; it is not part of the header.
constexpr std::string_view ROW_INDEX_COLUMN_NAME = "_row_index";

/// What a chunk column of this Arrow type looks like once the library has decoded it, and the
/// ClickHouse type it can then be copied into without converting anything. `std::nullopt` for a
/// type that has no such ClickHouse column - a decimal, a list, a date - which is what sends the
/// whole file through Arrow instead.
struct DirectType
{
    FFI_VortexArrayKind kind;
    FFI_VortexPrimitiveType ptype;
    DataTypePtr type;
};

std::optional<DirectType> directTypeOf(const arrow::DataType & arrow_type)
{
    auto primitive = [](FFI_VortexPrimitiveType ptype, DataTypePtr type)
    { return DirectType{FFI_VortexArrayKind::Primitive, ptype, std::move(type)}; };

    switch (arrow_type.id())
    {
        /// Vortex has no one-byte boolean: a `Bool` column is a bitmap, and ClickHouse's `UInt8`
        /// is the column it expands into.
        case arrow::Type::BOOL:
            return DirectType{FFI_VortexArrayKind::Bool, FFI_VortexPrimitiveType::U8, std::make_shared<DataTypeUInt8>()};
        case arrow::Type::INT8:
            return primitive(FFI_VortexPrimitiveType::I8, std::make_shared<DataTypeInt8>());
        case arrow::Type::INT16:
            return primitive(FFI_VortexPrimitiveType::I16, std::make_shared<DataTypeInt16>());
        case arrow::Type::INT32:
            return primitive(FFI_VortexPrimitiveType::I32, std::make_shared<DataTypeInt32>());
        case arrow::Type::INT64:
            return primitive(FFI_VortexPrimitiveType::I64, std::make_shared<DataTypeInt64>());
        case arrow::Type::UINT8:
            return primitive(FFI_VortexPrimitiveType::U8, std::make_shared<DataTypeUInt8>());
        case arrow::Type::UINT16:
            return primitive(FFI_VortexPrimitiveType::U16, std::make_shared<DataTypeUInt16>());
        case arrow::Type::UINT32:
            return primitive(FFI_VortexPrimitiveType::U32, std::make_shared<DataTypeUInt32>());
        case arrow::Type::UINT64:
            return primitive(FFI_VortexPrimitiveType::U64, std::make_shared<DataTypeUInt64>());
        case arrow::Type::FLOAT:
            return primitive(FFI_VortexPrimitiveType::F32, std::make_shared<DataTypeFloat32>());
        case arrow::Type::DOUBLE:
            return primitive(FFI_VortexPrimitiveType::F64, std::make_shared<DataTypeFloat64>());
        /// Both of Vortex's variable-length types decode to the same views, and ClickHouse reads
        /// both into a `String`.
        case arrow::Type::STRING:
        case arrow::Type::LARGE_STRING:
        case arrow::Type::STRING_VIEW:
        case arrow::Type::BINARY:
        case arrow::Type::LARGE_BINARY:
        case arrow::Type::BINARY_VIEW:
            return DirectType{FFI_VortexArrayKind::Binary, FFI_VortexPrimitiveType::U8, std::make_shared<DataTypeString>()};
        default:
            return std::nullopt;
    }
}

/// Expands `length` bits of `bits`, starting at `bit_offset`, into one byte apiece. `Invert` turns
/// a validity bitmap, where a set bit is a row that is not null, into a null map, where a set byte
/// is a row that is.
template <bool Invert>
void expandBits(const uint8_t * bits, UInt64 bit_offset, UInt8 * bytes, size_t length)
{
    for (size_t i = 0; i < length; ++i)
    {
        const UInt64 bit = bit_offset + i;
        const UInt8 value = (bits[bit / 8] >> (bit % 8)) & 1;
        bytes[i] = Invert ? value ^ 1 : value;
    }
}

template <typename T>
void fillVector(IColumn & column, const FFI_VortexColumnView & view)
{
    auto & data = assert_cast<ColumnVector<T> &>(column).getData();
    data.resize(view.length);
    if (view.length)
        memcpy(data.data(), view.values, view.length * sizeof(T));
}

void fillPrimitive(IColumn & column, const FFI_VortexColumnView & view)
{
    switch (view.ptype)
    {
        case FFI_VortexPrimitiveType::I8:
            return fillVector<Int8>(column, view);
        case FFI_VortexPrimitiveType::I16:
            return fillVector<Int16>(column, view);
        case FFI_VortexPrimitiveType::I32:
            return fillVector<Int32>(column, view);
        case FFI_VortexPrimitiveType::I64:
            return fillVector<Int64>(column, view);
        case FFI_VortexPrimitiveType::U8:
            return fillVector<UInt8>(column, view);
        case FFI_VortexPrimitiveType::U16:
            return fillVector<UInt16>(column, view);
        case FFI_VortexPrimitiveType::U32:
            return fillVector<UInt32>(column, view);
        case FFI_VortexPrimitiveType::U64:
            return fillVector<UInt64>(column, view);
        case FFI_VortexPrimitiveType::F32:
            return fillVector<Float32>(column, view);
        case FFI_VortexPrimitiveType::F64:
            return fillVector<Float64>(column, view);
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown Vortex primitive type {}", static_cast<int32_t>(view.ptype));
}

void fillBool(IColumn & column, const FFI_VortexColumnView & view)
{
    auto & data = assert_cast<ColumnUInt8 &>(column).getData();
    data.resize(view.length);
    if (view.length)
        expandBits<false>(view.values, view.values_bit_offset, data.data(), view.length);
}

/// The values of a variable-length column are the one thing the library writes rather than points
/// at: it lays them out end to end in the column's own memory, which is the layout ClickHouse
/// wants, so they are never written anywhere else on the way.
void fillString(IColumn & column, FFI_VortexChunk * chunk, size_t index, const FFI_VortexColumnView & view)
{
    auto & string_column = assert_cast<ColumnString &>(column);
    auto & chars = string_column.getChars();
    auto & offsets = string_column.getOffsets();
    chars.resize(view.total_value_bytes);
    offsets.resize(view.length);

    /// A `PaddedPODArray` always has this much allocated past its end, and a decoder that writes
    /// whole words rather than single bytes needs somewhere to put the tail of the last one. A
    /// column of nothing but empty values never allocated at all, so it gets no room either.
    const UInt64 capacity = view.total_value_bytes ? view.total_value_bytes + PADDING_FOR_SIMD : 0;

    /// No ClickHouse callback runs while a chunk is being decoded - the reads it needed are long
    /// over - so there is never a stashed exception to report instead of the message.
    char * error = nullptr;
    if (vortex_ffi_chunk_copy_binary(chunk, index, reinterpret_cast<uint8_t *>(chars.data()), capacity, offsets.data(), &error) != 0)
        throwVortexError(error, nullptr);
}

void fillNullMap(ColumnUInt8 & null_map, const FFI_VortexColumnView & view)
{
    auto & data = null_map.getData();
    data.resize(view.length);
    if (!view.length)
        return;
    if (view.validity)
        expandBits<true>(view.validity, view.validity_bit_offset, data.data(), view.length);
    else
        memset(data.data(), 0, view.length);
}

/// The row numbers of the chunk's rows, from the `_row_index` column the scan prepends. They are
/// the positions the rows had in the file, so a scan that kept every row of a range gives back a
/// range; anything else has to be spelled out as the filter that produced it.
std::shared_ptr<ChunkInfoRowNumbers> toRowNumbers(const FFI_VortexColumnView & view)
{
    const size_t num_rows = view.length;
    if (num_rows == 0)
        return std::make_shared<ChunkInfoRowNumbers>(0);

    const auto * row_numbers = reinterpret_cast<const UInt64 *>(view.values);
    const UInt64 first = row_numbers[0];
    const UInt64 last = row_numbers[num_rows - 1];
    if (last - first + 1 == num_rows)
        return std::make_shared<ChunkInfoRowNumbers>(first);

    auto info = std::make_shared<ChunkInfoRowNumbers>(first, IColumnFilter(last - first + 1, 0));
    for (size_t i = 0; i < num_rows; ++i)
        (*info->applied_filter)[row_numbers[i] - first] = 1;
    return info;
}

}

ColumnConverter::ColumnConverter(std::vector<ColumnPlan> plans_, bool has_row_index_column_)
    : plans(std::move(plans_)), has_row_index_column(has_row_index_column_)
{
}

std::unique_ptr<ColumnConverter>
ColumnConverter::create(const Block & header, const arrow::Schema & scan_schema, bool has_row_index_column)
{
    const size_t first_header_field = has_row_index_column ? 1 : 0;
    /// Every header column has to come from a column of the scan, and in the same order: this
    /// reads column i of the chunk into column i of the block and has nowhere to put anything
    /// else. A header naming a subcolumn, or a column the file does not have, breaks that.
    if (scan_schema.num_fields() != static_cast<int>(header.columns() + first_header_field))
        return nullptr;

    if (has_row_index_column && scan_schema.field(0)->name() != ROW_INDEX_COLUMN_NAME)
        return nullptr;

    std::vector<ColumnPlan> plans;
    plans.reserve(header.columns());
    for (size_t i = 0; i < header.columns(); ++i)
    {
        const auto & header_column = header.getByPosition(i);
        const auto & field = *scan_schema.field(static_cast<int>(i + first_header_field));
        if (field.name() != header_column.name)
            return nullptr;

        const auto direct = directTypeOf(*field.type());
        if (!direct)
            return nullptr;

        ColumnPlan plan;
        plan.kind = direct->kind;
        plan.ptype = direct->ptype;
        plan.type = header_column.type;
        plan.nullable = plan.type->isNullable();
        plan.nested_type = plan.nullable ? removeNullable(plan.type) : plan.type;

        if (!plan.nested_type->equals(*direct->type))
            return nullptr;
        /// A nullable column can only be read into a `Nullable` one; the other way round, where
        /// the file has no nulls and the header asked for a `Nullable` anyway, is a null map of
        /// zeroes. Everything else - `null_as_default` above all - belongs to the Arrow path.
        if (field.nullable() && !plan.nullable)
            return nullptr;

        plans.push_back(std::move(plan));
    }

    return std::unique_ptr<ColumnConverter>(new ColumnConverter(std::move(plans), has_row_index_column));
}

Chunk ColumnConverter::convert(FFI_VortexChunk * chunk) const
{
    const size_t num_columns = plans.size() + (has_row_index_column ? 1 : 0);
    std::vector<FFI_VortexColumnView> views(num_columns);

    /// This is where the split is decoded: the library takes every column out of the encoding the
    /// file stores it in and reports where the values ended up. Everything after it is the one
    /// copy into the block's own memory.
    Stopwatch decode_watch;
    char * error = nullptr;
    if (vortex_ffi_chunk_describe(chunk, views.data(), num_columns, &error) != 0)
        throwVortexError(error, nullptr);
    ProfileEvents::increment(ProfileEvents::VortexDecodeMicroseconds, decode_watch.elapsedMicroseconds());

    Stopwatch convert_watch;
    SCOPE_EXIT({ ProfileEvents::increment(ProfileEvents::VortexConvertMicroseconds, convert_watch.elapsedMicroseconds()); });

    const UInt64 num_rows = vortex_ffi_chunk_row_count(chunk);

    Columns columns;
    columns.reserve(plans.size());
    for (size_t i = 0; i < plans.size(); ++i)
    {
        const auto & plan = plans[i];
        const size_t index = i + (has_row_index_column ? 1 : 0);
        const auto & view = views[index];

        if (view.length != num_rows)
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "Column {} of the Vortex chunk has {} rows, and the chunk has {}",
                index,
                view.length,
                num_rows);
        /// The schema said one thing and the data is another, which only a corrupt file can do:
        /// the scan projects the file's own columns and the library decodes each of them by its
        /// declared type.
        if (view.kind != plan.kind || (plan.kind == FFI_VortexArrayKind::Primitive && view.ptype != plan.ptype))
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "Column {} of the Vortex chunk decoded as kind {} of type {}, and the file schema says it is kind {} of type {}",
                index,
                static_cast<int32_t>(view.kind),
                static_cast<int32_t>(view.ptype),
                static_cast<int32_t>(plan.kind),
                static_cast<int32_t>(plan.ptype));
        if (!plan.nullable && view.validity)
            throw Exception(
                ErrorCodes::INCORRECT_DATA, "Column {} of the Vortex chunk has nulls, and the file schema says it cannot", index);

        auto column = plan.nested_type->createColumn();
        switch (plan.kind)
        {
            case FFI_VortexArrayKind::Primitive:
                fillPrimitive(*column, view);
                break;
            case FFI_VortexArrayKind::Bool:
                fillBool(*column, view);
                break;
            case FFI_VortexArrayKind::Binary:
                fillString(*column, chunk, index, view);
                break;
        }

        if (plan.nullable)
        {
            auto null_map = ColumnUInt8::create();
            fillNullMap(*null_map, view);
            column = ColumnNullable::create(std::move(column), std::move(null_map));
        }
        columns.push_back(std::move(column));
    }

    Chunk result(std::move(columns), num_rows);
    if (has_row_index_column)
    {
        const auto & view = views[0];
        if (view.kind != FFI_VortexArrayKind::Primitive || view.ptype != FFI_VortexPrimitiveType::U64)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "The Vortex row index column is not a UInt64 one");
        result.getChunkInfos().add(toRowNumbers(view));
    }
    return result;
}

}

#endif
