CREATE TABLE source_greenplum
(
    t_bool      BOOLEAN,
    t_bytea     BYTES,
    t_char      CHAR,
    t_date      DATE,
    t_decimal   DECIMAL,
    t_float4    FLOAT,
    t_float8    DOUBLE,
    t_int2      SMALLINT,
    t_int4      INT,
    t_int8      BIGINT,
    t_numeric   DECIMAL,
    t_serial2   INT,
    t_serial4   INT,
    t_serial8   BIGINT,
    t_text      STRING,
    t_time      TIME,
    t_timestamp TIMESTAMP,
    t_timestamptz  TIMESTAMP,
    t_timetz    TIME,
    t_varchar   String
) WITH (
    'connector' = 'greenplum-x',
    'url' = 'jdbc:pivotal:greenplum://localhost:5432;DatabaseName=postgres',
    'table-name' = 'greenplum_all_type',
    'username' = 'gpadmin',
    'password' = 'gpadmin',
    'scan.fetch-size' = '2',
    'scan.query-timeout' = '10'
);

CREATE TABLE sink_greenplum (
    t_bool      BOOLEAN,
    t_bytea     BYTES,
    t_char      CHAR,
    t_date      DATE,
    t_decimal   DECIMAL,
    t_float4    FLOAT,
    t_float8    DOUBLE,
    t_int2      SMALLINT,
    t_int4      INT,
    t_int8      BIGINT,
    t_numeric   DECIMAL,
    t_serial2   INT,
    t_serial4   INT,
    t_serial8   BIGINT,
    t_text      STRING,
    t_time      TIME,
    t_timestamp TIMESTAMP,
    t_timestamptz  TIMESTAMP,
    t_timetz    TIME,
    t_varchar   String
) WITH (
      'connector' = 'greenplum-x',
      'url' = 'jdbc:pivotal:greenplum://localhost:5432;DatabaseName=postgres',
      'table-name' = 'greenplum_all_type_result',
      'username' = 'gpadmin',
      'password' = 'gpadmin',
      'sink.buffer-flush.max-rows' = '1',
      'sink.all-replace' = 'true'
);

INSERT INTO sink_greenplum
    SELECT * FROM source_greenplum;
