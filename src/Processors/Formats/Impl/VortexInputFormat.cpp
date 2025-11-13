#include "VortexInputFormat.h"
#include <Processors/Formats/Impl/ArrowColumnToCHColumn.h>
#include <Common/Exception.h>
#include <arrow/table.h>
#include <Formats/FormatFactory.h>

namespace DB
{

void registerInputFormatVortex(FormatFactory & factory)
{
    factory.registerRandomAccessInputFormat(
        "Vortex",
        [](ReadBuffer & buf,
           const Block & sample,
           const FormatSettings & settings,
           const ReadSettings &,
           bool,
           FormatParserSharedResourcesPtr,
           FormatFilterInfoPtr) -> InputFormatPtr
        {
            auto & seekable_buf = dynamic_cast<SeekableReadBuffer &>(buf);
            return std::make_shared<VortexBlockInputFormat>(
                seekable_buf,
                std::make_shared<const Block>(sample),
                settings);
        });
}

}  /// namespace DB
