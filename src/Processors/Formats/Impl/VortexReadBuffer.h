#pragma once

#include <IO/SeekableReadBuffer.h>
#include <IO/WithFileSize.h>
#include <vortex/file.hpp>
#include <vortex/io_simple.hpp>

namespace DB
{

class VortexReadBuffer : public vortex::io::VortexReadAtSimple
{
public:
    explicit VortexReadBuffer(SeekableReadBuffer & in_)
        : in(in_), file_size(getFileSizeFromReadBuffer(in_))
    {
    }

    size_t ReadAtInto(uint64_t pos, rust::Slice<uint8_t> buffer) const override
    {
        in.seek(pos, SEEK_SET);

        size_t len = buffer.size();
        in.readStrict(reinterpret_cast<char *>(buffer.data()), len);

        return len;
    }

    uint64_t GetSize() const override
    {
        return file_size;
    }

private:
    SeekableReadBuffer & in;
    size_t file_size;
};

} /// namespace DB
