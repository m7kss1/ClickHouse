#include "config.h"

#if USE_VORTEX

#if USE_ARROW

#include <Common/Exception.h>
#include <Core/Field.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeNullable.h>
#include <Formats/FormatFactory.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>
#include <Interpreters/convertFieldToType.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Processors/Formats/Impl/ArrowColumnToCHColumn.h>
#include <Processors/ISource.h>
#include <Processors/Sources/NullSource.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/IStorage.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/StorageFactory.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <arrow/c/abi.h>
#include <arrow/c/bridge.h>
#include <arrow/table.h>

#include <algorithm>
#include <memory>
#include <mutex>
#include <optional>
#include <utility>
#include <vector>

using FFI_ArrowSchema = ArrowSchema;
using FFI_ArrowArrayStream = ArrowArrayStream;
#define USE_OWN_ARROW 1
#include <vortex.h>
#undef USE_OWN_ARROW

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_READ_ALL_DATA;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int UNKNOWN_EXCEPTION;
}

namespace
{

String getVortexErrorMessage(const vx_error * error)
{
    if (!error)
        return "unknown Vortex error";

    const vx_string * message = vx_error_get_message(error);
    if (!message)
        return "unknown Vortex error";

    return {vx_string_ptr(message), vx_string_len(message)};
}

struct VortexExpressionDeleter
{
    void operator()(vx_expression * expression) const
    {
        if (expression)
            vx_expression_free(expression);
    }
};

using VortexExpressionPtr = std::unique_ptr<vx_expression, VortexExpressionDeleter>;

VortexExpressionPtr makeExpr(vx_expression * ptr)
{
    return VortexExpressionPtr(ptr, VortexExpressionDeleter{});
}

class VortexError
{
public:
    VortexError() = default;

    VortexError(const VortexError &) = delete;
    VortexError & operator=(const VortexError &) = delete;

    ~VortexError()
    {
        reset();
    }

    vx_error ** out()
    {
        reset();
        return &error;
    }

    explicit operator bool() const
    {
        return error != nullptr;
    }

    String message() const
    {
        return getVortexErrorMessage(error);
    }

    [[noreturn]] void throwException(const String & action) const
    {
        throw Exception(ErrorCodes::UNKNOWN_EXCEPTION, "{}: {}", action, message());
    }

private:
    void reset()
    {
        if (error)
        {
            vx_error_free(error);
            error = nullptr;
        }
    }

    vx_error * error = nullptr;
};

struct VortexSessionDeleter
{
    void operator()(vx_session * session) const
    {
        if (session)
            vx_session_free(session);
    }
};

struct VortexDataSourceDeleter
{
    void operator()(const vx_data_source * data_source) const
    {
        if (data_source)
            vx_data_source_free(data_source);
    }
};

struct VortexScanDeleter
{
    void operator()(vx_scan * scan) const
    {
        if (scan)
            vx_scan_free(scan);
    }
};

struct VortexScalarDeleter
{
    void operator()(vx_scalar * scalar) const
    {
        if (scalar)
            vx_scalar_free(scalar);
    }
};

using VortexSessionPtr = std::unique_ptr<vx_session, VortexSessionDeleter>;
using VortexDataSourcePtr = std::unique_ptr<const vx_data_source, VortexDataSourceDeleter>;
using VortexScanPtr = std::unique_ptr<vx_scan, VortexScanDeleter>;
using VortexScalarPtr = std::unique_ptr<vx_scalar, VortexScalarDeleter>;
using ArrowSchemaPtr = std::shared_ptr<arrow::Schema>;

VortexSessionPtr createVortexSession()
{
    /// vx_set_log_level(LOG_LEVEL_DEBUG);

    VortexSessionPtr session(vx_session_new());
    if (!session)
        throw Exception(ErrorCodes::UNKNOWN_EXCEPTION, "Failed to create Vortex session");
    return session;
}

VortexSessionPtr cloneVortexSession(const vx_session * session)
{
    VortexSessionPtr cloned(vx_session_clone(session));
    if (!cloned)
        throw Exception(ErrorCodes::UNKNOWN_EXCEPTION, "Failed to clone Vortex session");
    return cloned;
}

VortexDataSourcePtr createVortexDataSource(const vx_session * session, const String & paths)
{
    vx_data_source_options options{};
    options.paths = paths.c_str();

    VortexError error;
    VortexDataSourcePtr data_source(vx_data_source_new(session, &options, error.out()));
    if (error)
        error.throwException("Failed to create Vortex data source");
    if (!data_source)
        throw Exception(ErrorCodes::UNKNOWN_EXCEPTION, "Failed to create Vortex data source");

    return data_source;
}

std::shared_ptr<arrow::Schema> importVortexArrowSchema(const vx_dtype * dtype, const String & action)
{
    if (!dtype)
        throw Exception(ErrorCodes::UNKNOWN_EXCEPTION, "{}: missing Vortex dtype", action);

    ArrowSchema raw_schema{};
    VortexError error;
    if (vx_dtype_to_arrow_schema(dtype, &raw_schema, error.out()) != 0)
        error.throwException(action);
    if (error)
        error.throwException(action);

    auto schema = arrow::ImportSchema(&raw_schema);
    if (!schema.ok())
        throw Exception(
            ErrorCodes::UNKNOWN_EXCEPTION,
            "{}: {}",
            action,
            schema.status().ToString());

    return *schema;
}

ColumnsDescription inferColumnsFromVortex(const vx_data_source * data_source, const FormatSettings & format_settings)
{
    auto schema = importVortexArrowSchema(vx_data_source_dtype(data_source), "Failed to convert Vortex data source dtype to Arrow schema");
    auto header = ArrowColumnToCHColumn::arrowSchemaToCHHeader(*schema, nullptr, "Arrow", format_settings);
    return ColumnsDescription::fromNamesAndTypes(header.getNamesAndTypes());
}

class ArrowArrayStreamHolder
{
public:
    explicit ArrowArrayStreamHolder(ArrowArrayStream && stream_)
    {
        stream = stream_;
        stream_.release = nullptr;
    }

    ArrowArrayStreamHolder(const ArrowArrayStreamHolder &) = delete;
    ArrowArrayStreamHolder & operator=(const ArrowArrayStreamHolder &) = delete;

    ArrowArrayStreamHolder(ArrowArrayStreamHolder && other) noexcept
    {
        stream = other.stream;
        other.stream.release = nullptr;
    }

    ~ArrowArrayStreamHolder()
    {
        reset();
    }

    ArrowArrayStream * get()
    {
        return &stream;
    }

    const ArrowArrayStream * get() const
    {
        return &stream;
    }

private:
    void reset()
    {
        if (stream.release)
            stream.release(&stream);
        stream = {};
    }

    ArrowArrayStream stream{};
};

[[noreturn]] void throwArrowStreamException(const ArrowArrayStreamHolder & stream, const String & action)
{
    const char * last_error = nullptr;
    if (stream.get()->get_last_error)
        last_error = stream.get()->get_last_error(const_cast<ArrowArrayStream *>(stream.get()));

    throw Exception(
        ErrorCodes::CANNOT_READ_ALL_DATA,
        "{}{}{}",
        action,
        last_error ? ": " : "",
        last_error ? last_error : "");
}

struct VortexScanWithEstimate
{
    VortexSessionPtr session;
    VortexScanPtr scan;
    vx_estimate partition_count{};
    ArrowSchemaPtr arrow_schema;
};

class SharedVortexScan
{
public:
    SharedVortexScan(VortexSessionPtr session_, VortexScanPtr scan_, ArrowSchemaPtr arrow_schema_)
        : session(std::move(session_))
        , scan(std::move(scan_))
        , arrow_schema(std::move(arrow_schema_))
    {
    }

    vx_partition * nextPartition()
    {
        std::lock_guard lock(mutex);
        if (finished)
            return nullptr;

        VortexError error;
        vx_partition * partition = vx_scan_next_partition(scan.get(), error.out());
        if (error)
            error.throwException("Failed to read next Vortex partition");
        if (!partition)
            finished = true;

        return partition;
    }

    const vx_session * getSession() const
    {
        return session.get();
    }

    ArrowSchemaPtr getArrowSchema() const
    {
        return arrow_schema;
    }

private:
    VortexSessionPtr session;
    VortexScanPtr scan;
    ArrowSchemaPtr arrow_schema;
    std::mutex mutex;
    bool finished = false;
};

class VortexSource final : public ISource
{
public:
    VortexSource(SharedHeader header_, std::shared_ptr<SharedVortexScan> scan_, FormatSettings format_settings_)
        : ISource(header_)
        , header(std::move(header_))
        , scan(std::move(scan_))
        , arrow_schema(scan->getArrowSchema())
        , format_settings(std::move(format_settings_))
        , arrow_column_to_ch_column(std::make_unique<ArrowColumnToCHColumn>(
              *header,
              "Arrow",
              format_settings,
              std::nullopt,
              std::nullopt,
              format_settings.arrow.allow_missing_columns,
              format_settings.null_as_default,
              format_settings.date_time_overflow_behavior,
              format_settings.parquet.allow_geoparquet_parser,
              format_settings.arrow.case_insensitive_column_matching,
              true))
    {
    }

    String getName() const override { return "Vortex"; }

protected:
    Chunk generate() override
    {
        while (true)
        {
            if (!stream && !startNextPartitionStream())
                return {};

            ArrowArray raw_array{};
            if (stream->get()->get_next(stream->get(), &raw_array) != 0)
                throwArrowStreamException(*stream, "Failed to read next Arrow batch from Vortex partition");

            if (!raw_array.release)
            {
                stream.reset();
                continue;
            }

            auto record_batch = arrow::ImportRecordBatch(&raw_array, arrow_schema);
            if (!record_batch.ok())
                throw Exception(
                    ErrorCodes::CANNOT_READ_ALL_DATA,
                    "Failed to import Vortex Arrow batch: {}",
                    record_batch.status().ToString());

            auto table = arrow::Table::FromRecordBatches(std::vector<std::shared_ptr<arrow::RecordBatch>>{*record_batch});
            if (!table.ok())
                throw Exception(
                    ErrorCodes::CANNOT_READ_ALL_DATA,
                    "Failed to create Arrow table from Vortex batch: {}",
                    table.status().ToString());

            const size_t num_rows = (*table)->num_rows();
            if (num_rows == 0)
                continue;

            auto chunk = arrow_column_to_ch_column->arrowTableToCHChunk(*table, num_rows, nullptr, nullptr);
            progress(chunk.getNumRows(), chunk.bytes());
            return chunk;
        }
    }

private:
    bool startNextPartitionStream()
    {
        vx_partition * partition = scan->nextPartition();
        if (!partition)
            return false;

        ArrowArrayStream raw_stream{};
        VortexError error;
        const int rc = vx_partition_scan_arrow(scan->getSession(), partition, &raw_stream, error.out());
        partition = nullptr;

        if (rc != 0)
            error.throwException("Failed to convert Vortex partition to ArrowArrayStream");
        if (error)
            error.throwException("Failed to convert Vortex partition to ArrowArrayStream");

        ArrowArrayStreamHolder next_stream(std::move(raw_stream));
        stream.emplace(std::move(next_stream));
        return true;
    }

    SharedHeader header;
    std::shared_ptr<SharedVortexScan> scan;
    ArrowSchemaPtr arrow_schema;
    FormatSettings format_settings;
    std::unique_ptr<ArrowColumnToCHColumn> arrow_column_to_ch_column;
    std::optional<ArrowArrayStreamHolder> stream;
};

} // anonymous namespace

class StorageVortex final : public IStorage
{
public:
    StorageVortex(
        const StorageID & table_id_,
        ColumnsDescription columns_description_,
        ConstraintsDescription constraints_,
        const String & comment,
        String paths_,
        ContextPtr context)
        : IStorage(table_id_)
        , paths(std::move(paths_))
        , session(createVortexSession())
        , data_source(createVortexDataSource(session.get(), paths))
    {
        StorageInMemoryMetadata storage_metadata;
        if (columns_description_.empty())
            storage_metadata.setColumns(inferColumnsFromVortex(data_source.get(), getFormatSettings(context)));
        else
            storage_metadata.setColumns(columns_description_);

        storage_metadata.setConstraints(constraints_);
        storage_metadata.setComment(comment);
        setInMemoryMetadata(storage_metadata);
    }

    String getName() const override { return "Vortex"; }

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum,
        size_t max_block_size,
        size_t num_streams) override;

    std::optional<UInt64> totalRows(ContextPtr) const override
    {
        vx_estimate row_count{};
        vx_data_source_get_row_count(data_source.get(), &row_count);
        if (row_count.type == VX_ESTIMATE_EXACT)
            return row_count.estimate;
        return std::nullopt;
    }

    VortexScanWithEstimate createScan(
        const Names & column_names,
        size_t num_streams,
        const vx_expression * filter) const
    {
        VortexExpressionPtr root;
        VortexExpressionPtr projection;
        std::vector<const char *> projection_names;

        vx_scan_options options{};
        options.max_threads = num_streams;
        options.filter = filter;

        if (!column_names.empty())
        {
            root.reset(vx_expression_root());
            if (!root)
                throw Exception(ErrorCodes::UNKNOWN_EXCEPTION, "Failed to create Vortex root expression");

            projection_names.reserve(column_names.size());
            for (const auto & name : column_names)
                projection_names.push_back(name.c_str());

            projection.reset(vx_expression_select(projection_names.data(), projection_names.size(), root.get()));
            if (!projection)
                throw Exception(ErrorCodes::UNKNOWN_EXCEPTION, "Failed to create Vortex projection expression");

            options.projection = projection.get();
        }

        VortexError error;
        VortexScanWithEstimate result;
        result.session = cloneVortexSession(session.get());
        result.scan.reset(vx_data_source_scan(data_source.get(), &options, &result.partition_count, error.out()));
        if (error)
            error.throwException("Failed to create Vortex scan");
        if (!result.scan)
            throw Exception(ErrorCodes::UNKNOWN_EXCEPTION, "Failed to create Vortex scan");

        const vx_dtype * dtype = vx_scan_dtype(result.scan.get(), error.out());
        if (error)
            error.throwException("Failed to read Vortex scan dtype");
        result.arrow_schema = importVortexArrowSchema(dtype, "Failed to convert Vortex scan dtype to Arrow schema");

        return result;
    }

private:
    String paths;
    VortexSessionPtr session;
    VortexDataSourcePtr data_source;
};

class ReadFromVortex : public SourceStepWithFilter
{
public:
    std::string getName() const override { return "ReadFromVortex"; }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;
    void applyFilters(ActionDAGNodes added_filter_nodes) override;
    void updatePrewhereInfo(const PrewhereInfoPtr &) override {}

    ReadFromVortex(
        const Names & column_names_,
        const SelectQueryInfo & query_info_,
        const StorageSnapshotPtr & storage_snapshot_,
        const ContextPtr & context_,
        std::shared_ptr<StorageVortex> storage_,
        size_t num_streams_)
        : SourceStepWithFilter(
              std::make_shared<const Block>(storage_snapshot_->getSampleBlockForColumns(column_names_)),
              column_names_,
              query_info_,
              storage_snapshot_,
              context_)
        , storage(std::move(storage_))
        , num_streams(num_streams_)
    {
    }

private:
    VortexExpressionPtr createFilter(const ActionsDAG::Node * node);
    VortexExpressionPtr createLiteral(const ActionsDAG::Node * node, const DataTypePtr & target_type = nullptr);

    template <vx_binary_operator Op>
    VortexExpressionPtr createExprFromBinary(const ActionsDAG::Node * node);

    /// NOTE: skip_failed_children=true is only safe for AND
    /// For OR it would produce a subset data loss, so OR always passes false
    template <vx_expression * (*Func)(const vx_expression * const *, size_t)>
    VortexExpressionPtr createExprFromVariadic(const ActionsDAG::Node * node, bool skip_failed_children = false);

    std::shared_ptr<StorageVortex> storage;
    VortexExpressionPtr filter;
    VortexExpressionPtr filter_root;
    size_t num_streams;
};

void ReadFromVortex::applyFilters(ActionDAGNodes added_filter_nodes)
{
    SourceStepWithFilter::applyFilters(std::move(added_filter_nodes));

    const ActionsDAG::Node * predicate = filter_actions_dag
        ? filter_actions_dag->getOutputs().at(0)
        : nullptr;

    if (!predicate)
        return;

    filter_root = makeExpr(vx_expression_root());
    if (!filter_root)
        return;

    filter = createFilter(predicate);
}

void ReadFromVortex::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    auto scan_result = storage->createScan(required_source_columns, num_streams, filter.get());

    size_t stream_count = std::max<size_t>(1, num_streams);
    if (scan_result.partition_count.type != VX_ESTIMATE_UNKNOWN)
    {
        if (scan_result.partition_count.estimate == 0)
        {
            pipeline.init(Pipe(std::make_shared<NullSource>(output_header)));
            return;
        }
        stream_count = std::min<size_t>(stream_count, scan_result.partition_count.estimate);
    }

    auto shared_scan = std::make_shared<SharedVortexScan>(
        std::move(scan_result.session),
        std::move(scan_result.scan),
        std::move(scan_result.arrow_schema));

    Pipes pipes;
    pipes.reserve(stream_count);
    auto format_settings = getFormatSettings(getContext());
    for (size_t i = 0; i < stream_count; ++i)
        pipes.emplace_back(std::make_shared<VortexSource>(output_header, shared_scan, format_settings));

    pipeline.init(Pipe::unitePipes(std::move(pipes)));
}

VortexExpressionPtr ReadFromVortex::createLiteral(const ActionsDAG::Node * node, const DataTypePtr & target_type)
{
    if (!node->column || node->column->empty())
        return nullptr;

    Field field;
    node->column->get(0, field);

    if (field.isNull())
        return nullptr;

    const DataTypePtr & source_raw_type = node->result_type;

    if (WhichDataType(source_raw_type).isLowCardinality())
        return nullptr;
    if (target_type && WhichDataType(target_type).isLowCardinality())
        return nullptr;

    const DataTypePtr source_type = removeNullable(source_raw_type);

    DataTypePtr effective_type;
    bool is_nullable;

    if (target_type)
    {
        effective_type = removeNullable(target_type);
        is_nullable = target_type->isNullable();

        if (!effective_type->equals(*source_type))
        {
            Field converted = convertFieldToType(field, *effective_type, source_type.get());
            if (converted.isNull())
                return nullptr;
            field = std::move(converted);
        }
    }
    else
    {
        effective_type = source_type;
        is_nullable = source_raw_type->isNullable();
    }

    WhichDataType which(effective_type);

    VortexScalarPtr scalar;

    if (which.isUInt8())
        scalar.reset(vx_scalar_new_u8(static_cast<uint8_t>(field.safeGet<UInt64>()), is_nullable));
    else if (which.isUInt16())
        scalar.reset(vx_scalar_new_u16(static_cast<uint16_t>(field.safeGet<UInt64>()), is_nullable));
    else if (which.isUInt32())
        scalar.reset(vx_scalar_new_u32(static_cast<uint32_t>(field.safeGet<UInt64>()), is_nullable));
    else if (which.isUInt64())
        scalar.reset(vx_scalar_new_u64(field.safeGet<UInt64>(), is_nullable));
    else if (which.isInt8())
        scalar.reset(vx_scalar_new_i8(static_cast<int8_t>(field.safeGet<Int64>()), is_nullable));
    else if (which.isInt16())
        scalar.reset(vx_scalar_new_i16(static_cast<int16_t>(field.safeGet<Int64>()), is_nullable));
    else if (which.isInt32())
        scalar.reset(vx_scalar_new_i32(static_cast<int32_t>(field.safeGet<Int64>()), is_nullable));
    else if (which.isInt64())
        scalar.reset(vx_scalar_new_i64(field.safeGet<Int64>(), is_nullable));
    else if (which.isFloat32())
        scalar.reset(vx_scalar_new_f32(static_cast<float>(field.safeGet<Float64>()), is_nullable));
    else if (which.isFloat64())
        scalar.reset(vx_scalar_new_f64(field.safeGet<Float64>(), is_nullable));
    else if (which.isDate())
        scalar.reset(vx_scalar_new_u16(static_cast<uint16_t>(field.safeGet<UInt64>()), is_nullable));
    else if (which.isDate32())
        scalar.reset(vx_scalar_new_i32(static_cast<int32_t>(field.safeGet<Int64>()), is_nullable));
    else if (which.isDateTime())
        scalar.reset(vx_scalar_new_i64(static_cast<int64_t>(field.safeGet<UInt64>()), is_nullable));
    else if (which.isStringOrFixedString())
    {
        const auto & str = field.safeGet<String>();
        VortexError err;
        scalar.reset(vx_scalar_new_utf8(str.data(), str.size(), is_nullable, err.out()));
        if (err)
            return nullptr;
    }
    else
        return nullptr;

    if (!scalar)
        return nullptr;

    VortexError err;
    auto expr = makeExpr(vx_expression_literal(scalar.get(), err.out()));
    if (err || !expr)
        return nullptr;

    return expr;
}

VortexExpressionPtr ReadFromVortex::createFilter(const ActionsDAG::Node * node)
{
    if (!node)
        return nullptr;

    switch (node->type)
    {
        case ActionsDAG::ActionType::INPUT:
        {
            auto expr = makeExpr(vx_expression_get_item(node->result_name.c_str(), filter_root.get()));
            if (!expr)
                return nullptr;
            return expr;
        }

        case ActionsDAG::ActionType::ALIAS:
        {
            if (node->children.size() != 1)
                return nullptr;
            return createFilter(node->children[0]);
        }

        case ActionsDAG::ActionType::COLUMN:
        {
            if (!node->children.empty())
                return nullptr;
            return createLiteral(node);
        }

        case ActionsDAG::ActionType::FUNCTION:
        {
            const auto & name = node->function_base->getName();

            if (name == "equals")
                return createExprFromBinary<VX_OPERATOR_EQ>(node);
            if (name == "notEquals")
                return createExprFromBinary<VX_OPERATOR_NOT_EQ>(node);
            if (name == "greater")
                return createExprFromBinary<VX_OPERATOR_GT>(node);
            if (name == "greaterOrEquals")
                return createExprFromBinary<VX_OPERATOR_GTE>(node);
            if (name == "less")
                return createExprFromBinary<VX_OPERATOR_LT>(node);
            if (name == "lessOrEquals")
                return createExprFromBinary<VX_OPERATOR_LTE>(node);

            if (name == "and")
                return createExprFromVariadic<vx_expression_and>(node, /*skip_failed_children=*/true);
            if (name == "or")
                return createExprFromVariadic<vx_expression_or>(node, /*skip_failed_children=*/false);

            if (name == "not")
            {
                if (node->children.size() != 1)
                    return nullptr;
                auto child = createFilter(node->children[0]);
                if (!child)
                    return nullptr;
                const vx_expression * result = vx_expression_not(child.get());
                if (!result)
                    return nullptr;
                return makeExpr(const_cast<vx_expression *>(result));
            }

            if (name == "isNull")
            {
                if (node->children.size() != 1)
                    return nullptr;
                auto child = createFilter(node->children[0]);
                if (!child)
                    return nullptr;
                return makeExpr(vx_expression_is_null(child.get()));
            }

            if (name == "isNotNull")
            {
                if (node->children.size() != 1)
                    return nullptr;
                auto child = createFilter(node->children[0]);
                if (!child)
                    return nullptr;
                auto is_null_expr = makeExpr(vx_expression_is_null(child.get()));
                if (!is_null_expr)
                    return nullptr;
                const vx_expression * result = vx_expression_not(is_null_expr.get());
                if (!result)
                    return nullptr;
                return makeExpr(const_cast<vx_expression *>(result));
            }

            return nullptr;
        }

        default:
            return nullptr;
    }
}

template <vx_binary_operator Op>
VortexExpressionPtr ReadFromVortex::createExprFromBinary(const ActionsDAG::Node * node)
{
    if (node->children.size() != 2)
        return nullptr;

    const auto * lhs_raw = node->children[0];
    const auto * rhs_raw = node->children[1];

    const auto * lhs_inner = lhs_raw;
    while (lhs_inner && lhs_inner->type == ActionsDAG::ActionType::ALIAS)
        lhs_inner = lhs_inner->children.size() == 1 ? lhs_inner->children[0] : nullptr;

    const auto * rhs_inner = rhs_raw;
    while (rhs_inner && rhs_inner->type == ActionsDAG::ActionType::ALIAS)
        rhs_inner = rhs_inner->children.size() == 1 ? rhs_inner->children[0] : nullptr;

    if (!lhs_inner || !rhs_inner)
        return nullptr;

    /// A "constant" side is a materialized COLUMN node
    const bool lhs_const = lhs_inner->type == ActionsDAG::ActionType::COLUMN
        && lhs_inner->column && !lhs_inner->column->empty();
    const bool rhs_const = rhs_inner->type == ActionsDAG::ActionType::COLUMN
        && rhs_inner->column && !rhs_inner->column->empty();

    /// Both constants: constant folding should have handled this
    if (lhs_const && rhs_const)
        return nullptr;

    if (!lhs_const && !rhs_const)
    {
        /// Both sides are non-constant
        /// Only push when types are compatible
        const DataTypePtr lhs_type = removeNullable(lhs_raw->result_type);
        const DataTypePtr rhs_type = removeNullable(rhs_raw->result_type);
        if (WhichDataType(lhs_type).isLowCardinality() || WhichDataType(rhs_type).isLowCardinality())
            return nullptr;
        if (!lhs_type->equals(*rhs_type))
            return nullptr;

        auto lhs = createFilter(lhs_raw);
        auto rhs = createFilter(rhs_raw);
        if (!lhs || !rhs)
            return nullptr;
        return makeExpr(vx_expression_binary(Op, lhs.get(), rhs.get()));
    }

    const bool const_is_lhs = lhs_const;
    const auto * const_node = const_is_lhs ? lhs_inner : rhs_inner;
    const auto * col_raw = const_is_lhs ? rhs_raw : lhs_raw;

    auto col_expr = createFilter(col_raw);
    if (!col_expr)
        return nullptr;

    auto lit_expr = createLiteral(const_node, col_raw->result_type);
    if (!lit_expr)
        return nullptr;

    vx_expression * left = const_is_lhs ? lit_expr.get() : col_expr.get();
    vx_expression * right = const_is_lhs ? col_expr.get() : lit_expr.get();

    return makeExpr(vx_expression_binary(Op, left, right));
}

template <vx_expression * (*Func)(const vx_expression * const *, size_t)>
VortexExpressionPtr ReadFromVortex::createExprFromVariadic(const ActionsDAG::Node * node, bool skip_failed_children)
{
    if (node->children.empty())
        return nullptr;

    std::vector<VortexExpressionPtr> child_exprs;
    std::vector<const vx_expression *> child_ptrs;
    child_exprs.reserve(node->children.size());
    child_ptrs.reserve(node->children.size());

    for (const auto * child_node : node->children)
    {
        auto child_expr = createFilter(child_node);
        if (!child_expr)
        {
            if (skip_failed_children)
                continue;
            return nullptr;
        }

        child_ptrs.push_back(child_expr.get());
        child_exprs.emplace_back(std::move(child_expr));
    }

    if (child_ptrs.empty())
        return nullptr;

    return makeExpr(Func(child_ptrs.data(), child_ptrs.size()));
}

void StorageVortex::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum,
    size_t /*max_block_size*/,
    size_t num_streams)
{
    storage_snapshot->check(column_names);

    auto reading = std::make_unique<ReadFromVortex>(
        column_names,
        query_info,
        storage_snapshot,
        context,
        std::static_pointer_cast<StorageVortex>(shared_from_this()),
        num_streams);

    query_plan.addStep(std::move(reading));
}

void registerStorageVortex(StorageFactory & factory)
{
    [[maybe_unused]] auto * vortex_ffi_link_check = &vx_session_new;

    factory.registerStorage("Vortex", [](const StorageFactory::Arguments & args)
    {
        if (args.engine_args.size() != 1)
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Engine {} requires one argument: paths",
                args.engine_name);

        args.engine_args[0] = evaluateConstantExpressionAsLiteral(args.engine_args[0], args.getLocalContext());
        String paths = checkAndGetLiteralArgument<String>(args.engine_args[0], "paths");
        if (paths.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Engine {} paths argument must not be empty", args.engine_name);

        return std::make_shared<StorageVortex>(
            args.table_id, args.columns, args.constraints, args.comment, std::move(paths), args.getContext());
    },
    {
        .supports_schema_inference = true,
    });
}

}

#else

#include <Common/Exception.h>
#include <Storages/StorageFactory.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int SUPPORT_IS_DISABLED;
}

void registerStorageVortex(StorageFactory & factory)
{
    factory.registerStorage("Vortex", [](const StorageFactory::Arguments & args)
    {
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Engine {} requires Arrow support", args.engine_name);
    },
    {
        .supports_schema_inference = true,
    });
}

}

#endif

#endif
