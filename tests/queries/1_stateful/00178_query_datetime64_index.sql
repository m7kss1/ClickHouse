DROP TABLE IF EXISTS datetime64_index_tbl;

CREATE TABLE datetime64_index_tbl(ts DateTime64(3, 'UTC')) ENGINE=MergeTree ORDER BY ts;
INSERT INTO datetime64_index_tbl(ts) VALUES(toDateTime64('2023-05-27 00:00:00', 3, 'UTC'));

SELECT ts from datetime64_index_tbl where ts < toDate('2023-05-28');
SELECT ts from datetime64_index_tbl where ts < toDate32('2023-05-28');

DROP TABLE datetime64_index_tbl;
