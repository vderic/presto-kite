DROP TABLE IF EXISTS kite.default.all_ext;

CREATE TABLE kite.default.all_ext (
i8 tinyint,
i16 smallint,
i32 integer,
i64 bigint,
fp32 real,
fp64 double,
string varchar,
date date,
time time,
timestamp timestamp,
dec64 decimal(10,4),
dec128 decimal(25,4),
i8av array(tinyint),
i16av array(smallint),
i32av array(integer),
i64av array(bigint),
strav array(varchar),
fp32av array(real),
fp64av array(double),
dateav array(date),
timeav array(time),
timestampav array(timestamp),
dec64av array(decimal(16,4)),
dec128av array(decimal(25,4)))
WITH (format='parquet', LOCATION='kite://localhost:7878/test_presto/presto*.parquet');

