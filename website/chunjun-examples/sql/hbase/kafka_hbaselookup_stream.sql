CREATE TABLE k_source
(
    id        varchar,
    name      STRING,
    age       bigint,
    PROCTIME AS PROCTIME()
) WITH (
      'connector' = 'kafka-x'
      ,'topic' = 'da'
      ,'properties.bootstrap.servers' = 'localhost:9092'
      ,'properties.group.id' = 'mowen_g'
      ,'scan.startup.mode' = 'earliest-offset'
      ,'format' = 'json'
      ,'json.timestamp-format.standard' = 'SQL'
      );


CREATE TABLE hbase_lookup
(
    family1 ROW<col1 INT>,
    family2 ROW<col1 STRING, col2 BIGINT>,
    family3 ROW<col1 DOUBLE, col2 BOOLEAN, col3 STRING>,
    rowkey INT,
    PRIMARY KEY (rowkey) NOT ENFORCED
) WITH (
    'connector' = 'hbase14-x'
    ,'properties.zookeeper.quorum' = 'kudu1:2181,kudu2:2181,kudu3:2181'
    ,'properties.hbase.client.zookeeper.quorum' = 'kudu1:2181,kudu2:2181,kudu3:2181'
    ,'properties.zookeeper.znode.parent' = '/hbase_2.x'
    ,'null-string-literal' = 'null'
    ,'sink.buffer-flush.max-size' = '1000'
    ,'sink.buffer-flush.max-rows' = '1000'
    ,'sink.buffer-flush.interval' = '60'
    'lookup.cache-type' = 'lru'
);


CREATE TABLE sink
( id varchar,
  name varchar,
   age bigint,
   esid varchar,
   birthday timestamp)
WITH(
'connector' = 'stream-x'
);

INSERT INTO
sink
SELECT a.id,
       a.name,
       a.age,
       b.id as esid,
       b.birthday
FROM k_source a
LEFT JOIN hbase_lookup FOR SYSTEM_TIME AS OF a.PROCTIME AS b ON a.id = b.id;
