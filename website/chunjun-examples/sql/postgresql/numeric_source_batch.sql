CREATE TABLE source_pg
(
    val_smallint    SMALLINT,
    val_smallserial SMALLINT,
    val_int         INT,
    val_integer     INT,
    val_serial      INT,
    val_bigint      BIGINT,
    val_bigserial   BIGINT,
    val_oid         BIGINT,
    val_real        FLOAT,
    val_float       DOUBLE,
    val_double      DOUBLE,
    val_money       DOUBLE,
    val_decimal     DECIMAL,
    val_numeric     DECIMAL
) WITH (
    'connector' = 'postgresql-x',
    'url' = 'jdbc:postgresql://localhost:5432/dev',
    'table-name' = 'dim_numeric',
    'username' = 'postgres',
    'password' = 'root',
    'scan.fetch-size' = '2',
    'scan.query-timeout' = '10'
);

CREATE TABLE sink_stream (
     val_smallint    SMALLINT,
     val_smallserial SMALLINT,
     val_int         INT,
     val_integer     INT,
     val_serial      INT,
     val_bigint      BIGINT,
     val_bigserial   BIGINT,
     val_oid         BIGINT,
     val_real        FLOAT,
     val_float       DOUBLE,
     val_double      DOUBLE,
     val_money       DECIMAL,
     val_decimal     DECIMAL,
     val_numeric     DECIMAL
) WITH (
    'connector' = 'stream-x'
);

INSERT INTO sink_stream
    SELECT * FROM source_pg;
