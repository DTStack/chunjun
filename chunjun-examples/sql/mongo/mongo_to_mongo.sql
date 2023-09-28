CREATE TABLE lookup_mongo
(
    val_int       INT,
    val_long      BIGINT,
    val_double    DOUBLE
) WITH (
    'connector' = 'mongodb-x',
    --"url": "mongodb://mongo:0c4529b306988737@10.208.44.52:2908/crm?authSource=admin",
    'uri' = 'mongodb://localhost:27017',
    'username' = 'root',
    'password' = 'root',
    'database' = 'flink_dev',
    'collection' = 'dim_m',
    'filter'='{"val_int": {"$eq": 1}}',
    'scan.parallelism' = '1',
    'fetch-size'='0'
);

CREATE TABLE sink_print
(
    val_int       INT,
    val_long      BIGINT,
    val_double    DOUBLE
) WITH (
   'connector' = 'mongodb-x',
    'uri' = 'mongodb://localhost:27017',
    'database' = 'flink_dev',
    'collection' = 'test',
    'username' = 'root',
    'password' = 'root',
    'sink.parallelism' = '1',
    'sink.buffer-flush.max-rows' = '1024',
    'sink.buffer-flush.interval' = '10000'
);


insert into sink_print select * from lookup_mongo;
