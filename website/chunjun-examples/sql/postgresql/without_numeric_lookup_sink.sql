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
    val_character_varying STRING,
    val_varchar           STRING,
    val_character         STRING,
    val_char              STRING,
    val_text              STRING,
    val_name              STRING,
    val_bytea             BYTES,
    val_timestamp         TIMESTAMP,
    val_timestamptz       TIMESTAMP,
    val_date              DATE,
    val_time              TIME,
    val_timetz            TIME,
    val_boolean           BOOLEAN,
    PRIMARY KEY (val_varchar) NOT ENFORCED
) WITH (
    'connector' = 'postgresql-x',
    'url' = 'jdbc:postgresql://localhost:5432/dev',
    'table-name' = 'dim_without_numeric',
    'username' = 'postgres',
    'password' = 'root',
    'lookup.cache-type' = 'lru'
);

CREATE TABLE sink_pg (
    val_character_varying STRING,
    val_varchar           STRING,
    val_character         STRING,
    val_char              STRING,
    val_text              STRING,
    val_name              STRING,
    val_bytea             BYTES,
    val_timestamp         TIMESTAMP,
    val_timestamptz       TIMESTAMP,
    val_date              DATE,
    val_time              TIME,
    val_timetz            TIME,
    val_boolean           BOOLEAN
) WITH (
    'connector' = 'postgresql-x',
    'url' = 'jdbc:postgresql://localhost:5432/dev',
    'table-name' = 'dim_without_numeric',
    'username' = 'postgres',
    'password' = 'root',
    'sink.buffer-flush.max-rows' = '1',
    'sink.all-replace' = 'true'
);

CREATE TEMPORARY VIEW v AS
SELECT
    val_character_varying ,
    val_varchar           ,
    val_character         ,
    val_char              ,
    val_text              ,
    val_name              ,
    val_bytea             ,
    val_timestamp         ,
    val_timestamptz       ,
    val_date              ,
    val_time              ,
    val_timetz            ,
    val_boolean
FROM ods_k k
    JOIN
    lookup_pg FOR SYSTEM_TIME AS OF k.PROCTIME AS l
        ON k.name = l.val_varchar;

INSERT INTO sink_pg
    SELECT * FROM v;
