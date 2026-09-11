#pragma once

#include "config.h"

#if USE_VORTEX

#include <Core/Block.h>
#include <Processors/Chunk.h>

#include <memory>
#include <vector>

namespace arrow
{
class Schema;
}

struct FFI_VortexChunk;
enum class FFI_VortexArrayKind : int32_t;
enum class FFI_VortexPrimitiveType : int32_t;

namespace DB::Vortex
{

/// Reads the columns of a Vortex chunk straight into ClickHouse columns.
///
/// A chunk arrives in the encodings the file stores it in. The library decodes it into the
/// canonical ones - a bit-packed integer column becomes a buffer of integers, an FSST string
/// column becomes views over its decompressed bytes - and reports where the values are; this then
/// writes them into the columns of the block, once. Neither half builds an Arrow array, which is
/// what used to lay every value out a second time on the way across.
///
/// Only the types whose Vortex and ClickHouse representations differ by nothing but that copy are
/// handled: integers, floats, booleans, strings, and any of them nullable. `create` returns
/// nothing for a header with anything else in it, and such a file is read through Arrow instead.
class ColumnConverter
{
public:
    /// A converter when every column of the header can be filled from the matching column of the
    /// scan, and nothing when even one of them cannot.
    ///
    /// `scan_schema` is what the scan delivers: the header's columns that the file has, in the
    /// header's order, with `_row_index` first when the query asked for row numbers.
    static std::unique_ptr<ColumnConverter>
    create(const Block & header, const arrow::Schema & scan_schema, bool has_row_index_column);

    /// The chunk of the header's columns, with the row numbers of its rows attached when the scan
    /// was asked for them. Runs on the thread that decoded the split.
    Chunk convert(FFI_VortexChunk * chunk) const;

private:
    /// What one column of a chunk is read into.
    struct ColumnPlan
    {
        /// What the chunk's column has to be for the header's type to be filled from it. The
        /// library decodes each column on its own and a file can disagree with its own schema, so
        /// this is checked against every chunk rather than trusted.
        FFI_VortexArrayKind kind;
        FFI_VortexPrimitiveType ptype;
        DataTypePtr type;
        /// What is inside the `Nullable`, or `type` itself when it is not one.
        DataTypePtr nested_type;
        bool nullable = false;
    };

    ColumnConverter(std::vector<ColumnPlan> plans_, bool has_row_index_column_);

    const std::vector<ColumnPlan> plans;
    /// The scan puts the row numbers in a column of their own, ahead of the header's.
    const bool has_row_index_column;
};

}

#endif
