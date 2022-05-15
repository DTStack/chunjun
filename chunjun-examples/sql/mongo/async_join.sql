-- {"id":100,"name":"lb james阿道夫","money":293.899778,"dateone":"2020-07-30 10:08:22","age":"33","datethree":"2020-07-30 10:08:22.123","datesix":"2020-07-30 10:08:22.123456","datenigth":"2020-07-30 10:08:22.123456789","dtdate":"2020-07-30","dttime":"10:08:22"}
CREATE TABLE ods_k
(
    id   INT,
    name STRING,
    PROCTIME AS PROCTIME()
) WITH (
    'connector' = 'kafka-x',
    'topic' = 'luna',
    'properties.bootstrap.servers' = 'localhost:9092',
    'properties.group.id' = 'luna_g',
    'format' = 'json',
    'scan.startup.mode' = 'latest-offset',
    'json.timestamp-format.standard' = 'SQL'
);

CREATE TABLE lookup_mongo
(
    val_int       INT,
    val_long      BIGINT,
    val_double    DOUBLE,
    val_decimal   DECIMAL,
    `_id`         STRING,
    val_str       STRING,
    val_bindata VARBINARY,
    val_date      DATE,
    val_timestamp TIMESTAMP,
    val_bool      BOOLEAN,
    PRIMARY KEY (val_int) NOT ENFORCED
) WITH (
    'connector' = 'mongodb-x',
    'url' = 'mongodb://localhost:27017',
    'database' = 'flink_dev',
    'collection' = 'dim_m',
    'lookup.cache-type' = 'lru'
);

CREATE TABLE sink_print
(
    val_int       INT,
    val_long      BIGINT,
    val_double    DOUBLE,
    val_decimal   DECIMAL,
    `_id`         STRING,
    val_str       STRING,
    val_bindata VARBINARY,
    val_date      DATE,
    val_timestamp TIMESTAMP,
    val_bool      BOOLEAN
) WITH (
    'connector' = 'print'
);


INSERT INTO sink_print
SELECT val_int,
       val_long,
       val_double,
       val_decimal,
       `_id`,
       val_str,
       val_bindata,
       val_date,
       val_timestamp,
       val_bool
FROM ods_k k
         LEFT JOIN
     lookup_mongo FOR SYSTEM_TIME AS OF k.PROCTIME AS l
ON k.id = l.val_int;


