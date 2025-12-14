#pragma once

#include <Processors/Formats/IInputFormat.h>
#include <Processors/Formats/ISchemaReader.h>
#include <Processors/Formats/Impl/ArrowBlockInputFormat.h>
#include <Processors/Formats/Impl/ArrowColumnToCHColumn.h>
#include <Formats/FormatFilterInfo.h>
#include <Core/BlockMissingValues.h>
#include <Core/Field.h>
#include <Formats/FormatSettings.h>
#include <Core/Range.h>
#include <Common/Exception.h>
#include <base/types.h>
#include <arrow/c/abi.h>
#include <arrow/c/bridge.h>
#include <arrow/table.h>
#include <vortex/scan.hpp>
#include <vortex/expr.hpp>
#include <vortex/scalar.hpp>

#include <Storages/MergeTree/KeyCondition.h>

#include <string_view>
#include <vector>
#include <optional>

#include "VortexReadBuffer.h"

namespace DB 
{

/** Stream for reading data in Vortex format. **/
class VortexBlockInputFormat : public IInputFormat
{
public:
    VortexBlockInputFormat(
        SeekableReadBuffer & in_,
        SharedHeader header_,
        FormatFilterInfoPtr format_filter_info_,
        const FormatSettings & format_settings_
    )
        : IInputFormat(header_, &in_)
        , format_filter_info(std::move(format_filter_info_))
        , block_missing_values(getPort().getHeader().columns())
        , format_settings(format_settings_)
    {
        memset(&stream, 0, sizeof(ArrowArrayStream));
    }

    String getName() const override { return "VortexBlockInputFormat"; }

    void resetParser() override
    {
        IInputFormat::resetParser();

        if (stream.release)
        {
            stream.release(&stream);
            memset(&stream, 0, sizeof(ArrowArrayStream));
        }

        schema_initialized = false;
        file.reset();
        schema.reset();
        arrow_column_to_ch_column.reset();
        block_missing_values.clear();
    }

private:

    void initializeVortex()
    {
        chassert(!file);
        if (format_filter_info)
        {
            format_filter_info->initOnce([&]
            {
                format_filter_info->initKeyCondition(getPort().getHeader());
            });
        }

        auto & seekable_in = dynamic_cast<SeekableReadBuffer &>(*in);
        auto reader = std::make_unique<VortexReadBuffer>(seekable_in);
        file = std::make_shared<vortex::VortexFile>(vortex::VortexFile::OpenSeekableSimple(std::move(reader)));
    }

    void initializeArrowStream()
    {
        chassert(!schema_initialized && file);

        auto scan_builder = file->CreateScanBuilder();
        applyProjectionIfNeeded(scan_builder);
        applyFilterIfNeeded(scan_builder);
        stream = std::move(scan_builder).IntoStream();
        ArrowSchema arrow_schema{};

        int rc = stream.get_schema(&stream, &arrow_schema);
        chassert(!rc);

        schema = arrow::ImportSchema(&arrow_schema).ValueOrDie();
        
        arrow_column_to_ch_column = std::make_unique<ArrowColumnToCHColumn>(
            getPort().getHeader(), "Vortex", format_settings,
            std::nullopt, std::nullopt,
            true, format_settings.null_as_default,
            format_settings.date_time_overflow_behavior,
            false, false, false, true
        );

        schema_initialized = true;
    }
    
    Chunk read() override
    {
        if (!file)
            initializeVortex();
        if (!schema_initialized)
            initializeArrowStream();

        ArrowArray arrow_array;
        memset(&arrow_array, 0, sizeof(ArrowArray));

        int rc = stream.get_next(&stream, &arrow_array);
        if (rc != 0 || arrow_array.release == nullptr)
            return {};

        chassert(!rc);

        auto record_batch = arrow::ImportRecordBatch(&arrow_array, schema).ValueOrDie();
        auto table = arrow::Table::FromRecordBatches({record_batch}).ValueOrDie();

        return arrow_column_to_ch_column->arrowTableToCHChunk(
            table, table->num_rows(), nullptr,
            format_settings.defaults_for_omitted_fields ? &block_missing_values : nullptr
        );
    }

    void applyProjectionIfNeeded(vortex::ScanBuilder & scan_builder)
    {
        const auto column_names = getPort().getHeader().getNames();
        if (column_names.empty())
            return;

        std::vector<std::string_view> requested_columns;
        requested_columns.reserve(column_names.size());
        for (const auto & name : column_names)
            requested_columns.emplace_back(name);

        auto projection = vortex::expr::select(requested_columns, vortex::expr::root());
        scan_builder.WithProjection(std::move(projection));
    }

    /// Filter pushdown flow:
    ///      SQL filter --> ActionsDAG --> KeyCondition (RPN) -->
    ///      --> Vortex Expression Tree --> ScanBuilder.WithFilter()
    void applyFilterIfNeeded(vortex::ScanBuilder & scan_builder)
    {
        if (!format_filter_info || !format_filter_info->hasFilter() || !format_filter_info->key_condition)
            return;

        auto expr = buildFilterExprFromKeyCondition(*format_filter_info->key_condition);
        if (expr)
            scan_builder.WithFilter(std::move(*expr));
    }

    /// KeyCondition contains information about which columns are used in the key.
    /// It's implemented as RPN (Reverse Polish Notation) expression over simple functions:
    /// e.g. SELECT * FROM table WHERE toDate(p) >= '2020-09-01' AND p <= '2020-09-01 00:00:00'
    ///      --> {FUNCTION_IN_RANGE, FUNCTION_IN_RANGE, FUNCTION_AND}
    /// We translate each supported atom in RPM into expression nodes and building
    /// the same logical tree that ScanBuilder consumes internally.
    /// Once attached, the Vortex engine relies on its statistics to prune row groups before generating Arrow batches,
    /// so all we need to do is provide a correct expression tree
    std::optional<vortex::expr::Expr> buildFilterExprFromKeyCondition(const KeyCondition & key_condition)
    {
        const auto & rpn = key_condition.getRPN();
        if (rpn.empty())
            return std::nullopt;

        std::vector<String> columns_by_index(key_condition.getNumKeyColumns());
        for (const auto & [name, index] : key_condition.getKeyColumns())
        {
            if (index >= columns_by_index.size())
                columns_by_index.resize(index + 1);
            columns_by_index[index] = name;
        }

        std::vector<vortex::expr::Expr> stack;
        stack.reserve(rpn.size());

        for (const auto & element : rpn)
        {
            using Function = KeyCondition::RPNElement::Function;
            switch (element.function)
            {
                case Function::FUNCTION_IN_RANGE:
                case Function::FUNCTION_NOT_IN_RANGE:
                {
                    if (element.key_column >= columns_by_index.size())
                        return std::nullopt;

                    const auto & column_name = columns_by_index[element.key_column];
                    if (column_name.empty())
                        return std::nullopt;

                    std::optional<vortex::expr::Expr> range_expr;
                    if (element.function == Function::FUNCTION_IN_RANGE)
                    {
                        range_expr = buildRangeExpr</* is_not_in_range */false>(column_name, element.range);
                    }
                    else 
                    {
                        range_expr = buildRangeExpr</* is_not_in_range */true>(column_name, element.range);
                    }
                    if (!range_expr)
                        return std::nullopt;

                    stack.emplace_back(std::move(*range_expr));
                    break;
                }
                case Function::FUNCTION_NOT:
                {
                    if (stack.empty())
                        return std::nullopt;
                    auto value = std::move(stack.back());
                    stack.pop_back();
                    stack.emplace_back(vortex::expr::not_(std::move(value)));
                    break;
                }
                case Function::FUNCTION_AND:
                {
                    if (stack.size() < 2)
                        return std::nullopt;
                    auto rhs = std::move(stack.back());
                    stack.pop_back();
                    auto lhs = std::move(stack.back());
                    stack.pop_back();
                    stack.emplace_back(vortex::expr::and_(std::move(lhs), std::move(rhs)));
                    break;
                }
                case Function::FUNCTION_OR:
                {
                    if (stack.size() < 2)
                        return std::nullopt;
                    auto rhs = std::move(stack.back());
                    stack.pop_back();
                    auto lhs = std::move(stack.back());
                    stack.pop_back();
                    stack.emplace_back(vortex::expr::or_(std::move(lhs), std::move(rhs)));
                    break;
                }
                case Function::FUNCTION_IS_NULL:
                case Function::FUNCTION_IS_NOT_NULL:
                {
                    if (element.key_column >= columns_by_index.size())
                        return std::nullopt;

                    const auto & column_name = columns_by_index[element.key_column];
                    if (column_name.empty())
                        return std::nullopt;

                    auto column_expr = vortex::expr::column(column_name);
                    if (element.function == Function::FUNCTION_IS_NULL)
                        stack.emplace_back(vortex::expr::is_null(std::move(column_expr)));
                    else
                        stack.emplace_back(vortex::expr::not_(vortex::expr::is_null(std::move(column_expr))));
                    break;
                }
                case Function::ALWAYS_FALSE:
                {
                    stack.emplace_back(vortex::expr::literal(vortex::scalar::bool_(false)));
                    break;
                }
                case Function::ALWAYS_TRUE:
                {
                    stack.emplace_back(vortex::expr::literal(vortex::scalar::bool_(true)));
                    break;
                }
                case Function::FUNCTION_UNKNOWN:
                case Function::FUNCTION_IN_SET:
                case Function::FUNCTION_NOT_IN_SET:
                case Function::FUNCTION_POINT_IN_POLYGON:
                case Function::FUNCTION_ARGS_IN_HYPERRECTANGLE:
                {
                    /// Unsupported yet
                    return std::nullopt;
                }
                
            }
        }

        if (stack.size() != 1)
            return std::nullopt;
        return std::move(stack.back());
    }

    template<bool is_not_in_range>
    std::optional<vortex::expr::Expr> buildRangeExpr(const String & column_name, const Range & range) const
    {
        std::optional<vortex::expr::Expr> left_expr;
        std::optional<vortex::expr::Expr> right_expr;

        auto make_literal = [&](const FieldRef & field_ref) -> std::optional<vortex::expr::Expr>
        {
            if (field_ref.isNull())
                return std::nullopt;
            auto scalar = fieldToScalar(static_cast<const Field &>(field_ref));
            if (!scalar)
                return std::nullopt;
            return vortex::expr::literal(std::move(*scalar));
        };

        auto make_column = [&]() { return vortex::expr::column(column_name); };
        
        if (!range.left.isNegativeInfinity())
        {
            auto literal = make_literal(range.left);
            if (!literal)
                return std::nullopt;
            
            if constexpr (is_not_in_range) 
            {
                left_expr = range.left_included
                    ? vortex::expr::lt(make_column(), std::move(*literal))
                    : vortex::expr::lt_eq(make_column(),  std::move(*literal));
            }
            else
            {
                left_expr = range.left_included
                    ? vortex::expr::gt_eq(make_column(), std::move(*literal))
                    : vortex::expr::gt(make_column(),  std::move(*literal));
            }
        }

        if (!range.right.isPositiveInfinity())
        {
            auto literal = make_literal(range.right);
            if (!literal)
                return std::nullopt;

            if constexpr (is_not_in_range)
            {
                right_expr = range.right_included
                    ? vortex::expr::gt(make_column(), std::move(*literal))
                    : vortex::expr::gt_eq(make_column(),  std::move(*literal));
            }
            else
            {
                right_expr = range.right_included
                    ? vortex::expr::lt_eq(make_column(), std::move(*literal))
                    : vortex::expr::lt(make_column(),  std::move(*literal));
            }
        }

        if constexpr (is_not_in_range)
        {
            if (left_expr && right_expr)
            {
                /// NOT (L <= x <= R) -> (x < L OR x > R)
                /// NOT (L < x < R) -> (x <= L OR x >= R)
                return vortex::expr::or_(std::move(*left_expr), std::move(*right_expr));
            }
            return left_expr.has_value() ? std::move(*left_expr) : std::move(*right_expr);
        }
        else
        {
            if (left_expr && right_expr)
            {
                return vortex::expr::and_(std::move(*left_expr), std::move(*right_expr));
            }
            return left_expr.has_value() ? std::move(*left_expr) : std::move(*right_expr);
        }
        
    }

    static std::optional<vortex::scalar::Scalar> fieldToScalar(const Field & field)
    {
        switch (field.getType())
        {
            case Field::Types::UInt64:
                return vortex::scalar::uint64(field.safeGet<UInt64>());
            case Field::Types::Int64:
                return vortex::scalar::int64(field.safeGet<Int64>());
            case Field::Types::Float64:
                return vortex::scalar::float64(field.safeGet<Float64>());
            case Field::Types::String:
                return vortex::scalar::string(field.safeGet<String>());
            default:
                /// TODO: Implement me
                break;
        }
        return std::nullopt;
    }

    std::shared_ptr<vortex::VortexFile> file;
    std::shared_ptr<arrow::Schema> schema;
    ArrowArrayStream stream;
    Block header;
    FormatFilterInfoPtr format_filter_info;
    bool schema_initialized = false;

    std::unique_ptr<ArrowColumnToCHColumn> arrow_column_to_ch_column;
    BlockMissingValues block_missing_values;
    const FormatSettings format_settings;
};

class VortexSchemaReader : public ISchemaReader
{
public:
    VortexSchemaReader(SeekableReadBuffer & in_, const FormatSettings & format_settings_)
        : ISchemaReader(in_)
        , format_settings(format_settings_)
    {
    }

    ~VortexSchemaReader() override
    {
        if (stream.release)
        {
            stream.release(&stream);
            memset(&stream, 0, sizeof(ArrowArrayStream));
        }
    }

    NamesAndTypesList readSchema() override
    {
        initializeIfNeeded();

        auto header = ArrowColumnToCHColumn::arrowSchemaToCHHeader(
            *schema,
            nullptr,
            "Vortex",
            format_settings,
            format_settings.arrow.skip_columns_with_unsupported_types_in_schema_inference,
            format_settings.schema_inference_make_columns_nullable != 0,
            false,
            format_settings.parquet.allow_geoparquet_parser);

        return header.getNamesAndTypesList();
    }

    std::optional<size_t> readNumberOrRows() override
    {
        initializeIfNeeded();
        if (!file) return std::nullopt;
        return static_cast<size_t>(file->RowCount());
    }

private:
    void initializeIfNeeded()
    {
        if (schema) return;

        auto & seekable_in = dynamic_cast<SeekableReadBuffer &>(in);
        auto reader = std::make_unique<VortexReadBuffer>(seekable_in);
        file = std::make_shared<vortex::VortexFile>(vortex::VortexFile::OpenSeekableSimple(std::move(reader)));
        stream = std::move(file->CreateScanBuilder()).IntoStream();

        ArrowSchema arrow_schema{};
        int rc = stream.get_schema(&stream, &arrow_schema);
        chassert(!rc);
        schema = arrow::ImportSchema(&arrow_schema).ValueOrDie();
    }

    const FormatSettings format_settings;
    std::shared_ptr<vortex::VortexFile> file;
    std::shared_ptr<arrow::Schema> schema;
    ArrowArrayStream stream;
};

} /// namespace DB
