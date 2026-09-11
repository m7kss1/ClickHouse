#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# ^ the Vortex format is not included in the fast test and MSan builds

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The columns of a chunk are written into the block's own columns, with no Arrow array in between,
# for every type whose Vortex and ClickHouse representations differ by nothing but that copy. This
# pins down what comes back that way: enough rows to cross several splits, values that stress the
# string layout, and nulls in every nullable column.
DATA_FILE=$CUR_DIR/test_$CLICKHOUSE_TEST_UNIQUE_NAME.vortex

$CLICKHOUSE_LOCAL -q "
    SELECT
        number::Int8 AS i8,
        number::Int16 AS i16,
        number::Int32 AS i32,
        number::Int64 AS i64,
        number::UInt8 AS u8,
        number::UInt16 AS u16,
        number::UInt32 AS u32,
        number::UInt64 AS u64,
        (number / 8)::Float32 AS f32,
        (number / 8)::Float64 AS f64,
        number % 2 = 0 AS b,
        -- Empty, inlined (12 bytes or fewer) and out-of-line values in the same column, which is
        -- what a string column is laid out from.
        repeat('x', number % 40) AS s,
        if(number % 3 = 0, NULL, number::Int32) AS ni32,
        if(number % 5 = 0, NULL, repeat('y', number % 41)) AS ns,
        if(number % 7 = 0, NULL, number % 2 = 0) AS nb
    FROM numbers(300000)
    FORMAT Vortex" > "$DATA_FILE"

echo "The inferred types:"
$CLICKHOUSE_LOCAL -q "DESC file('$DATA_FILE', 'Vortex')" | cut -f1,2

echo "The values come back unchanged:"
$CLICKHOUSE_LOCAL -q "
    SELECT
        count(),
        sum(i8), sum(i16), sum(i32), sum(i64),
        sum(u8), sum(u16), sum(u32), sum(u64),
        round(sum(f32)), sum(f64), sum(b),
        sum(cityHash64(s)), max(length(s)),
        sum(ni32), count(ni32),
        sum(cityHash64(ns)), count(ns), countIf(ns = ''),
        sum(nb), count(nb)
    FROM file('$DATA_FILE', 'Vortex')" | tr '\t' '\n'

echo "A column of nothing but empty and null strings:"
$CLICKHOUSE_LOCAL -q "
    SELECT number AS n, '' AS empty, CAST(NULL, 'Nullable(String)') AS nothing
    FROM numbers(1000)
    FORMAT Vortex" > "$DATA_FILE".empty
$CLICKHOUSE_LOCAL -q "
    SELECT count(), countIf(empty = ''), count(nothing), sum(n)
    FROM file('$DATA_FILE.empty', 'Vortex')"

# A header naming a subcolumn cannot be filled from a column of the chunk one for one, so the whole
# scan falls back to reading its chunks as Arrow arrays. The answers have to be the same either way.
echo "A header the direct conversion does not cover reads the same values:"
$CLICKHOUSE_LOCAL -q "
    SELECT number AS n, tuple(number, repeat('z', number % 30))::Tuple(a UInt64, b String) AS t
    FROM numbers(100000)
    FORMAT Vortex" > "$DATA_FILE".nested
$CLICKHOUSE_LOCAL -q "SELECT sum(\`t.a\`), sum(cityHash64(\`t.b\`)) FROM file('$DATA_FILE.nested', 'Vortex', '\`t.a\` UInt64, \`t.b\` String')"
$CLICKHOUSE_LOCAL -q "SELECT sum(t.1), sum(cityHash64(t.2)) FROM file('$DATA_FILE.nested', 'Vortex')"

echo "A pushed-down filter narrows the same values down the same way:"
for push_down in 1 0; do
    $CLICKHOUSE_LOCAL -q "
        SELECT count(), sum(i64), sum(cityHash64(s)), count(ns)
        FROM file('$DATA_FILE', 'Vortex')
        WHERE i32 >= 100000 AND i32 < 200000
        SETTINGS input_format_vortex_filter_push_down = $push_down"
done

rm -f "$DATA_FILE" "$DATA_FILE".empty "$DATA_FILE".nested
