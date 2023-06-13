CREATE TABLE lookup_mongo
(
    val_int       INT,
    val_long      BIGINT,
    val_double    DOUBLE
) WITH (
    'connector' = 'mongodb-x',
    'uri' = 'mongodb://localhost:27017',
    'database' = 'flink_dev',
    'collection' = 'dim_m',
    -- 'filter'='{"val_int": {"$eq": 1}}',
    'scan.parallelism' = '2',
    'fetch-size'='0'
);

CREATE TABLE sink_print
(
    val_int       INT,
    val_long      BIGINT,
    val_double    DOUBLE
) WITH (
    'connector' = 'print'
);


insert into sink_print select * from lookup_mongo;
