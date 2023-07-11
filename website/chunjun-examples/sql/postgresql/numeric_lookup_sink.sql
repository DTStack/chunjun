-- {"id":100,"name":"lb james阿道夫","money":293.899778,"dateone":"2020-07-30 10:08:22","age":"33","datethree":"2020-07-30 10:08:22.123","datesix":"2020-07-30 10:08:22.123456","datenigth":"2020-07-30 10:08:22.123456789","dtdate":"2020-07-30","dttime":"10:08:22"}
CREATE TABLE ods_k
(
    id        INT,
    name      STRING,
    PROCTIME AS PROCTIME()
) WITH (
    'connector' = 'kafka-x'
    ,'topic' = 'luna'
    ,'properties.bootstrap.servers' = 'localhost:9092'
    ,'properties.group.id' = 'luna_g'
    -- ,'scan.startup.mode' = 'earliest-offset'
    -- ,'scan.startup.mode' = 'latest-offset'
    ,'format' = 'json'
    ,'json.timestamp-format.standard' = 'SQL'
);

CREATE TABLE lookup_pg
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
    val_numeric     DECIMAL,
    PRIMARY KEY (val_smallint) NOT ENFORCED
) WITH (
    'connector' = 'postgresql-x',
    'url' = 'jdbc:postgresql://localhost:5432/dev',
    'table-name' = 'dim_numeric',
    'username' = 'postgres',
    'password' = 'root',
    'lookup.cache-type' = 'lru'
);

CREATE TABLE sink_pg (
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
    'connector' = 'postgresql-x',
    'url' = 'jdbc:postgresql://localhost:5432/dev',
    'table-name' = 'dim_numeric',
    'username' = 'postgres',
    'password' = 'root',
    'sink.buffer-flush.max-rows' = '1',
    'sink.all-replace' = 'true'
);

CREATE TEMPORARY VIEW v
AS
SELECT
    val_smallint    ,
    val_smallserial ,
    val_int         ,
    val_integer     ,
    val_serial      ,
    val_bigint      ,
    val_bigserial   ,
    val_oid         ,
    val_real        ,
    val_float       ,
    val_double      ,
    val_money       ,
    val_decimal     ,
    val_numeric
FROM ods_k k
    JOIN
    lookup_pg FOR SYSTEM_TIME AS OF k.PROCTIME AS l
        ON k.id = l.val_int;

INSERT INTO sink_pg
    SELECT * FROM v;
