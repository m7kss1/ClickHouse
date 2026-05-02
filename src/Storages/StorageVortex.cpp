#include "config.h"

#if USE_VORTEX

#if USE_ARROW

#include <Common/Exception.h>
#include <Formats/FormatFactory.h>
#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Processors/Formats/Impl/ArrowColumnToCHColumn.h>
#include <Processors/ISource.h>
#include <Processors/Sources/NullSource.h>
#include <QueryPipeline/Pipe.h>
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

struct VortexExpressionDeleter
{
    void operator()(vx_expression * expression) const
    {
        if (expression)
            vx_expression_free(expression);
    }
};

using VortexSessionPtr = std::unique_ptr<vx_session, VortexSessionDeleter>;
using VortexDataSourcePtr = std::unique_ptr<const vx_data_source, VortexDataSourceDeleter>;
using VortexScanPtr = std::unique_ptr<vx_scan, VortexScanDeleter>;
using VortexExpressionPtr = std::unique_ptr<vx_expression, VortexExpressionDeleter>;
using ArrowSchemaPtr = std::shared_ptr<arrow::Schema>;

VortexSessionPtr createVortexSession()
{
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

}

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

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo &,
        ContextPtr context,
        QueryProcessingStage::Enum,
        size_t,
        size_t num_streams) override
    {
        storage_snapshot->check(column_names);

        auto header = std::make_shared<const Block>(storage_snapshot->getSampleBlockForColumns(column_names));
        auto scan_result = createScan(column_names, num_streams);

        size_t stream_count = std::max<size_t>(1, num_streams);
        if (scan_result.partition_count.type != VX_ESTIMATE_UNKNOWN)
        {
            if (scan_result.partition_count.estimate == 0)
                return Pipe(std::make_shared<NullSource>(header));
            stream_count = std::min<size_t>(stream_count, scan_result.partition_count.estimate);
        }

        auto shared_scan = std::make_shared<SharedVortexScan>(
            cloneVortexSession(session.get()), std::move(scan_result.scan), std::move(scan_result.arrow_schema));

        Pipes pipes;
        pipes.reserve(stream_count);
        auto format_settings = getFormatSettings(context);
        for (size_t i = 0; i < stream_count; ++i)
            pipes.emplace_back(std::make_shared<VortexSource>(header, shared_scan, format_settings));

        return Pipe::unitePipes(std::move(pipes));
    }

    std::optional<UInt64> totalRows(ContextPtr) const override
    {
        vx_estimate row_count{};
        vx_data_source_get_row_count(data_source.get(), &row_count);
        if (row_count.type == VX_ESTIMATE_EXACT)
            return row_count.estimate;
        return std::nullopt;
    }

private:
    VortexScanWithEstimate createScan(const Names & column_names, size_t num_streams) const
    {
        VortexExpressionPtr root;
        VortexExpressionPtr projection;
        std::vector<const char *> projection_names;

        vx_scan_options options{};
        options.max_threads = num_streams;

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

    String paths;
    VortexSessionPtr session;
    VortexDataSourcePtr data_source;
};

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
