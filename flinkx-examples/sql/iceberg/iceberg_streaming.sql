CREATE CATALOG iceberg_hive WITH (
    'type'='iceberg',
    'catalog-type'='hive',
    'uri'='thrift://kudu3:9083',
    'clients'='2',
    'property-version'='1',
    'warehouse'='hdfs://ns1//user/hive/warehouse'
);

CREATE TABLE IF NOT EXISTS iceberg_hive.luna.dwd (
    id BIGINT COMMENT 'unique id',
    name STRING
);

CREATE TABLE IF NOT EXISTS iceberg_hive.luna.ads (
    id BIGINT COMMENT 'unique id',
    name STRING
);

CREATE TABLE ods_k (
    id BIGINT,
    name STRING
) WITH (
    'connector' = 'kafka-x',
    'topic' = 'luna',
    'properties.bootstrap.servers' = 'localhost:9092',
    'format' = 'json',
    'json.timestamp-format.standard' = 'SQL'
);

CREATE TABLE v (
   id BIGINT,
   name STRING
) WITH (
    'connector' = 'print'
);

INSERT INTO iceberg_hive.luna.dwd SELECT * FROM ods_k;

INSERT INTO iceberg_hive.luna.ads SELECT * FROM iceberg_hive.luna.dwd
/*+ OPTIONS('streaming'='true', 'monitor-interval'='1s')*/;

INSERT INTO v SELECT * FROM iceberg_hive.luna.dwd
/*+ OPTIONS('streaming'='true', 'monitor-interval'='1s')*/
