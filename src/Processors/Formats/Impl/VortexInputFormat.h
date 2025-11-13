#pragma once

#include <Processors/Formats/IInputFormat.h>
#include <Processors/Formats/ISchemaReader.h>
#include <Processors/Formats/Impl/ArrowBlockInputFormat.h>
#include <Processors/Formats/Impl/ArrowColumnToCHColumn.h>
#include <Core/BlockMissingValues.h>
#include <Formats/FormatSettings.h>
#include <Common/Exception.h>
#include <arrow/c/abi.h>
#include <arrow/c/bridge.h>
#include <arrow/table.h>
#include <vortex/scan.hpp>

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
        const FormatSettings & format_settings_
    )
        : IInputFormat(header_, &in_)
        , format_settings(format_settings_)
    {
        memset(&stream, 0, sizeof(ArrowArrayStream));
        memset(&arrow_schema, 0, sizeof(ArrowSchema));
    }

    String getName() const override { return "VortexBlockInputFormat"; }

    void resetParser() override
    {
        IInputFormat::resetParser();

        if (arrow_schema.release)
        {
            arrow_schema.release(&arrow_schema);
            memset(&arrow_schema, 0, sizeof(ArrowSchema));
        }

        if (stream.release)
        {
            stream.release(&stream);
            memset(&stream, 0, sizeof(ArrowArrayStream));
        }

        schema_initialized = false;
        file.reset();
        arrow_column_to_ch_column.reset();
        block_missing_values.clear();
    }

private:

    void initializeVortex()
    {
        chassert(!file);
        auto & seekable_in = dynamic_cast<SeekableReadBuffer &>(*in);
        auto reader = std::make_unique<VortexReadBuffer>(seekable_in);
        file = std::make_shared<vortex::VortexFile>(vortex::VortexFile::OpenSeekableSimple(std::move(reader)));
    }

    void initializeArrowStream()
    {
        chassert(!schema_initialized && file);

        stream = std::move(file->CreateScanBuilder()).IntoStream();

        int rc = stream.get_schema(&stream, &arrow_schema);
        if (rc != 0)
            chassert(false);

        auto maybe_schema = arrow::ImportSchema(&arrow_schema);
        if (!maybe_schema.ok())
            chassert(false);

        auto header = ArrowColumnToCHColumn::arrowSchemaToCHHeader(
            *maybe_schema.ValueOrDie(),
            nullptr, "Vortex", format_settings,
            false, true, false, false
        );

        arrow_column_to_ch_column = std::make_unique<ArrowColumnToCHColumn>(
            header, "Vortex", format_settings,
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

        auto record_batch = arrow::ImportRecordBatch(&arrow_array, &arrow_schema).ValueOrDie();
        auto table = arrow::Table::FromRecordBatches({record_batch}).ValueOrDie();

        return arrow_column_to_ch_column->arrowTableToCHChunk(
            table, table->num_rows(), nullptr,
            format_settings.defaults_for_omitted_fields ? &block_missing_values : nullptr
        );
    }

    std::shared_ptr<vortex::VortexFile> file;
    ArrowArrayStream stream;
    ArrowSchema arrow_schema;
    bool schema_initialized = false;

    std::unique_ptr<ArrowColumnToCHColumn> arrow_column_to_ch_column;
    BlockMissingValues block_missing_values;
    const FormatSettings format_settings;
};

// class VortexSchemaReader : public ISchemaReader
// {
// public:
//     VortexSchemaReader(SeekableReadBuffer & in_, const FormatSettings & format_settings_);

//     NamesAndTypesList readSchema() override;
//     std::optional<size_t> readNumberOrRows() override;

// private:
//     const FormatSettings format_settings;
//     std::unique_ptr<VortexBufferedStream> reader;
// };

} /// namespace DB



