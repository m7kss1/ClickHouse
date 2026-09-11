# Vortex format

Reading and writing [Vortex](https://vortex.dev/) files. Vortex is a columnar format for compressed
Arrow-compatible data: its encodings and layouts are extensible, statistics are kept per zone, and a
scan takes a filter and a projection, so a selective query never has to decode the whole file.

The support has two halves:

* **this directory** - the `Vortex` input and output formats, the schema reader, and the translation
  of a ClickHouse `WHERE` condition into a Vortex filter expression;
* **`rust/workspace/vortex`** - a ClickHouse-owned crate that wraps the `vortex` crate (0.84.0,
  features `files` and `zstd`) in a C API. It is the only code that talks to the format itself.

A scan delivers the columns of a split as they come out of the file, and
`VortexColumnConverter.{h,cpp}` writes them into the block's own columns - once, with no Arrow array
in between. Schemas cross the boundary as
[Arrow C Data Interface](https://arrow.apache.org/docs/format/CDataInterface.html) structs, and so
do arrays being written and chunks of a type the direct conversion does not cover. Everything else -
IO, threads, backpressure, cancellation - is delegated back to ClickHouse through callbacks: the
crate owns no thread and opens no file.

## Files

| File | Contents |
|------|----------|
| `VortexBlockInputFormat.{h,cpp}` | `SELECT`: drives the scan, converts chunks, hands them to the pipeline. Also `VortexSchemaReader` and the input format registration, including the user-facing documentation string. |
| `VortexBlockOutputFormat.{h,cpp}` | `INSERT` / `INTO OUTFILE`: chunks out as Arrow record batches. |
| `VortexColumnConverter.{h,cpp}` | A decoded chunk into the block's columns, for the types that need nothing but the copy. |
| `VortexScanPlanner.{h,cpp}` | What one scan is asked to do: which columns to read, which rows to keep. |
| `VortexExpressionConverter.{h,cpp}` | The filter `ActionsDAG` to a Vortex expression, when it can be translated soundly. |
| `VortexFFIHelpers.{h,cpp}` | Opening a reader, the read callback, error and exception plumbing. |
| `rust/workspace/vortex/include/vortex_ffi.h` | The C API and its contract. Hand-written. |
| `rust/workspace/vortex/src/lib.rs` | Its implementation, plus the crate tests. |

## Building

`ENABLE_VORTEX` defaults to `ENABLE_LIBRARIES` and additionally needs `ch_contrib::arrow`; MSan
builds force it off, because the instrumented library makes `unit_tests_dbms` overflow the
`R_X86_64_PC32` relocations of the small code model, and the whole Rust part is skipped on FreeBSD.
`src/configure_config.cmake` turns the resulting `ch_rust::vortex` target into `USE_VORTEX`.

The sources here are compiled unconditionally: with `USE_VORTEX` off, everything is `#if`-ed out and
what is left are the empty registration functions, so `registerFormats` links either way.

## The FFI boundary

`vortex_ffi.h` is the contract; the rules that matter when changing either side:

* **The header is generated from `lib.rs`; do not edit it.** The two are separate translation units
  linked by symbol name, so a signature changed on one side only would be silent undefined
  behaviour rather than a compile error. `rust/workspace/vortex/build.rs` regenerates the header
  with `cbindgen` on every build and fails when the committed one differs, so forgetting cannot go
  unnoticed. After changing anything the header exposes, run
  `VORTEX_FFI_HEADER_UPDATE=1 cargo check -p _ch_rust_vortex` from `rust/workspace` and commit the
  result. The file comment and the Arrow C Data Interface definitions - everything that cannot be
  derived from Rust - live in `rust/workspace/vortex/cbindgen.toml`.
* **Ownership.** An Arrow struct passed *into* a function is consumed by it; one written to an
  out-parameter belongs to the caller, who has to release it (importing it does that). The chunk
  passed to `on_chunk`, and everything `vortex_ffi_chunk_describe` points at, are only *borrowed*
  for the duration of the call.
* **Errors travel as strings** in a `char **` out-parameter and are freed with
  `vortex_ffi_free_string`. Where the failure really happened inside a ClickHouse callback, the
  original exception is stashed in `VortexReadContext` and rethrown instead of the generic message.
* **Nothing unwinds across the boundary.** Every C entry point on this side (`vortexFFIReadCallback`,
  `vortexFFIChunkCallback`, `vortexFFINotifyCallback`, `vortexFFIWriteCallback`) is `noexcept` and
  reports failure by return value; on the Rust side `ffi_wrap` catches both errors and panics.

## Threading

The library spawns futures; it does not run them. `FFI_VortexRuntime` is two queues of
`async-task` runnables plus a callback that says a queue became runnable, and a runnable only ever
executes inside `vortex_ffi_runtime_run`. ClickHouse decides who runs what, when, and on how many
threads:

```
   vortex_ffi_runtime_new(this, vortexFFINotifyCallback)
                       │
                       ▼
      ┌──────────────────┬───────────────────┐
      │    CPU queue     │     IO queue      │
      │ filter, project, │ calls that block  │
      │ assemble a chunk │ on a read         │
      └──────────────────┴───────────────────┘
               ▲                  ▲
        onNotify(CPU)       onNotify(IO)      "a task became runnable" - schedules a driver
               │                  │
       driveQueue(CPU)      driveQueue(IO)    runs tasks in batches until both queues are empty
    on parsing_runner       on io_runner
    (max_parsing_threads)   (max_download_threads)
```

`onNotify` starts a driver only while this reader has fewer than its share of the pool running
(`getParsingThreadsPerReader`, `getIOThreadsPerReader` - recomputed, because files read in parallel
divide the pool and get their share back as the others finish). The handshake between `onNotify` and
the end of `driveQueue` is sequentially consistent on both sides: a task queued just as a driver
leaves is seen by one of the two, or it would sit in the queue with nobody to run it. That is also
why `read` re-checks every second and re-arms the drivers rather than hang; both "deadlock in the
Vortex reader" `LOGICAL_ERROR`s are watchdogs for a bug in this protocol.

Two degenerate cases fall out of the same design. With `max_download_threads = 0` there is no
separate IO pool, so the CPU drivers alternate between both queues - draining the decode queue first
would leave the next splits waiting for reads nobody started. With `max_parsing_threads = 1` there
is no pool at all: `read` runs the tasks itself between the waits, and "nothing ran and nothing can
run" is then a real deadlock.

## Reading a file

`prepareReader` runs on the first `read`:

1. `openVortexReader` wraps the `ReadBuffer` in an Arrow file (`asArrowFile`, read-ahead off - the
   library picks its own byte ranges) and opens the reader. The footer is read on the calling
   thread, so nothing has to be driving the queues yet. `makeReaderOptions` picks the IO
   parameters: concurrent reads only for a buffer that supports positioned reads (`BufferReader`,
   `RandomAccessFileFromRandomAccessReadBuffer`), otherwise one read at a time; neighbouring
   segments up to 1 MiB apart are merged into one request, capped at 4 MiB locally and 16 MiB on
   remote storage.
2. `planVortexScan` picks the columns - the header's columns that the file actually has, plus the
   whole field behind any `name.sub` subcolumn - and builds the pushed-down filter.
3. `vortex_ffi_scan_create` optimizes the expression, computes the splits, and starts the scan.

From then on the data flows without anyone asking for it:

```
split task (CPU) ──► read_at ──► IO task ──► vortexFFIReadCallback ──► ReadBuffer
     │
     └─ filter, project
              │
              ▼
        onChunk(chunk, split_index)          still on the thread that read the split,
              │  VortexColumnConverter       so decoding and conversion are parallel too
              ▼
        delivered[split_index]
              │
              ▼
            read()  ──► returns the Chunk, then vortex_ffi_scan_release(1)
```

**Conversion.** A chunk arrives in the encodings the file stores it in.
`vortex_ffi_chunk_describe` decodes each column into its canonical one - a bit-packed integer column
becomes a buffer of integers, an FSST string column becomes views over its decompressed bytes - and
says where the values are; `VortexColumnConverter` then writes them into the block's columns. That
is the only time the values are written: the string columns are laid out by
`vortex_ffi_chunk_copy_binary` directly in `ColumnString`'s own `chars` and `offsets`, and an FSST
column is decompressed straight into them rather than into a buffer of Vortex's own first.

Only the types whose Vortex and ClickHouse representations differ by nothing but that copy are
handled this way: integers, floats, booleans, strings, and any of them nullable, each read into the
header column of exactly that type. `ColumnConverter::create` returns nothing for a header with
anything else in it - a `Date`, a `Decimal`, a subcolumn, a column the file does not have - and then
every chunk of that scan goes through `vortex_ffi_chunk_export_arrow` and `ArrowColumnToCHColumn`
instead, which lays every value out a second time. `VortexDecodeMicroseconds` and
`VortexConvertMicroseconds` split the two halves apart, and the decision is in the log at `test`
level.

**Backpressure.** `max_splits_in_flight` (2 per decoding thread, clamped to 4..64) bounds the splits
being read, decoded or waiting for `read`. A permit is only returned when `read` hands the chunk out,
so the scan cannot run far ahead of the query. It counts splits, not bytes - a wide projection makes
each of them large, which is why the limit is low.

**Order.** Splits are delivered as they finish, and `read` takes whatever is ready, so the row order
of a file is not the file's own. `input_format_vortex_preserve_order` makes `read` wait for
`next_split_index` instead; a slow split then holds back the ones behind it. A split filtered down to
nothing is still delivered, as an empty entry, because the order needs it.

**Teardown.** Cancellation can come from any thread, so the scan handle lives under `scan_mutex`.
`closeReader` cancels the scan (which drains the queues, and that is what makes the drivers stop),
waits for the drivers on `ShutdownHelper`, and only then frees the scan, the reader and the runtime -
in that order, since each outlives the next. A driver still waiting in the pool when the reader is
destroyed finds the shutdown and returns without touching it.

## Filter pushdown

`input_format_vortex_filter_push_down` (on by default) translates the query's filter `ActionsDAG` and
gives it to the scan, which drops the rows it rules out and skips the statistics zones it excludes.

The contract that makes this safe: **the pushed-down filter may keep more rows than the condition,
never fewer.** ClickHouse reapplies the full `WHERE` to the result anyway, so a conjunct that does
not translate is simply dropped, and a partial translation is *widened*. Widening is only sound in a
positive position - under a `not` every node has to translate exactly, which the `allow_widening`
flag enforces.

`buildFilter` pushes the NOTs into the atoms first (`ActionsDAGWithInversionPushDown`), then splits
the predicate at the top-level `and` and translates each conjunct on its own, so one untranslatable
conjunct does not cost the others. `VortexExpressionConverter` handles `and`, `or`, `not`, the six
comparisons, `isNull`, `isNotNull`, `in`/`notIn` (and their `global` forms), `like`, `notLike`,
`startsWith`, and a bare boolean column. `nullIn` is deliberately absent: it matches `NULL` against
`NULL`, while a Vortex comparison with `NULL` yields `NULL` and keeps nothing.

An atom translates only when the comparison provably means the same thing on both sides.
`typesMatchForFilterPushdown` is the gate, and it is stricter than it looks:

* `UInt64` in the header over `I64` in the file has the same bits but a different order, so it does
  not qualify - and neither does any other signedness mismatch;
* `Bool` over `U8` clamps every non-zero value to 1 when read, so the raw bytes compare differently;
* `Date32` is excluded under `date_time_overflow_behavior = 'saturate'`, where out-of-range days are
  clamped onto the bounds and an equality on a bound would match rows it should not;
* a `DateTime64` matches a `vortex.timestamp` only at the same scale, since any other scale makes the
  decoder rescale the values; the header's time zone does not matter, it only affects rendering;
* `FixedString` is excluded because its zero padding orders differently from `Binary`.

Literals are then built in the file column's own type and only if the value fits it exactly - Vortex
compares only same-typed operands, and a rounded bound would change which rows match. `LIKE` with a
literal prefix becomes `column >= prefix AND column < <next string after the prefix>`; when the
right-hand bound cannot be built, the range degrades to its left half, which is a widening.

Adding one more function is one entry in the dispatch table; adding one more literal type is one case
in the type-matching and literal-building switches, usually behind a new `vortex_ffi_expr_literal_*`.

## Writing

Writing is ordinary and single-threaded: chunks go out as Arrow record batches, and the library
chooses the encodings and the layout and streams the bytes back through `vortexFFIWriteCallback`.
Its runtime is private and driven on the calling thread.

The `CHColumnToArrowColumn` settings encode the type decisions: `String` and `FixedString` leave as
`Binary` (a ClickHouse string is any byte sequence, while Vortex requires `Utf8` to be valid UTF-8,
and there is no fixed-width binary type), `DateTime` leaves as a second-precision timestamp so it
comes back as a temporal type rather than a number, and `Nothing` maps to the Vortex `Null` type.
A file with no rows at all is still written with the header's schema.

## Schema inference and `count`

`VortexSchemaReader` opens the file the same way but with a runtime it drives itself, and answers
both `readSchema` and `readNumberOrRows` from the footer. The settings that shape an inferred type
are part of the schema cache key (`registerAdditionalInfoForSchemaCacheGetter`) - a cached schema is
only valid for the same values of them.

`SELECT count()` never creates a scan: `need_only_count` answers from `vortex_ffi_reader_row_count`,
and a query that needs some columns but none the file has (`plan.column_names` empty) gets empty
chunks of the right size from `readWithoutColumns`.

## Settings, events, logs

| Name | Effect |
|------|--------|
| `input_format_vortex_filter_push_down` | Translate what can be translated of the `WHERE` and give it to the scan. Default on. |
| `input_format_vortex_preserve_order` | Return the rows in file order, at the cost of holding chunks back. Default off. |
| `max_parsing_threads` | Threads that decode, shared by the files a query reads in parallel. `1` disables the pool. |
| `max_download_threads` | Threads that read. `0` makes the reads share the decoding threads. |

`ProfileEvents`: `VortexFilterPushdownConjunctsPushed` and `VortexFilterPushdownConjunctsDropped`
(how much of the condition actually reached the scan), `VortexScanSplits` and `VortexScanEmptySplits`
(how many splits the filter emptied out), `VortexDecodeMicroseconds` and `VortexConvertMicroseconds`
(decoding a split out of the file's encodings, and writing it into the block's columns),
`VortexReadRequests`, `VortexReadBytes` and `VortexReadWaitMicroseconds`. The scan plan - columns,
rendered filter, conjuncts pushed of total - and which way the chunks are converted are logged at
`TEST` level by `VortexBlockInputFormat`. In a stack trace or `top` the pool threads appear as
`VortexDecoder` and `VortexReader`.

## Tests

Stateless tests, all tagged `no-fasttest, no-msan` because the format is absent from those builds:

| Test | Covers |
|------|--------|
| `04669_vortex_format` | The round trip over the supported types, schema inference, and the count from the footer. |
| `05104_vortex_direct_conversion` | What the chunks read straight into ClickHouse columns come back as, and that a header with no direct conversion gives the same values. |
| `04761_vortex_datetime` | `DateTime` comes back as `DateTime64(0)`, not as a number. |
| `04812_vortex_ipv4` | `IPv4` goes out as `U32` and is inferred back as `UInt32`. |
| `04815_vortex_null` | `SELECT NULL` writes the Vortex `Null` type. |
| `04821_vortex_filter_pushdown` | Pushdown answers match the scan without it, and it reaches the scan. |
| `04891_vortex_async_insert`, `04892_vortex_insert_sparse` | What the all-formats matrices leave out. |
| `04893_vortex_schema_cache` | Every setting that shapes an inferred type is in the cache key. |
| `04927_vortex_parallel_read` | Parallel reads do not change the answers, and preserve order when asked. |
| `05030_vortex_pushdown_types` | Pushdown over every type and comparison, against the same query without it. |
| `05031_vortex_pushdown_profile_events` | The `ProfileEvents` prove which conjuncts were pushed. |

The crate has its own tests, including the scan protocol under several worker threads:

```
cd rust/workspace && cargo test -p _ch_rust_vortex --offline
```

## User-facing documentation

`docs/reference/formats/Vortex.mdx` is generated. The source of truth is the `setDocumentation` call
in `registerInputFormatVortex` at the bottom of `VortexBlockInputFormat.cpp` - the type matching
table, the settings and the performance notes live there. Edit the C++ string, never the region
between the `AUTOGENERATED_START` and `AUTOGENERATED_END` markers in the `.mdx`.

## Not implemented

* **Row ranges.** `FFI_VortexScanOptions` has `row_range_begin`/`row_range_end` and the scan honours
  them, but nothing sets them yet; they are what a bucket or a `LIMIT` would use.
* **Lazy materialization.** `FormatFilterInfo::rows_to_read` is ignored, and `PREWHERE` steps are not
  evaluated on the scan's output.
* **Nested pushdown.** Only top-level columns are pushed down; a predicate on a subcolumn stays with
  ClickHouse.
* **One scan per reader.** Everything that bounds the reads is set up per scan, so a second one on
  the same reader is refused.
* **Types Vortex has no counterpart for** - `Map`, `Int128`/`UInt128`/`Int256`/`UInt256`, `IPv6`,
  `Interval` - cannot be written at all.
