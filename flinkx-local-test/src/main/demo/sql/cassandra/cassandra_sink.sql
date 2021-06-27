CREATE TABLE source_one
(
    id        int,
    name      varchar,
    age       int,
    birth     timestamp,
    todayTime time,
    todayDate date,
    money     decimal,
    price     double,
    wechat    varchar,
    proName   varchar
) WITH (
      'connector' = 'kafka-x'
      ,'topic' = 'tiezhu_test'
      ,'properties.bootstrap.servers' = 'kudu1:9092'
      ,'properties.group.id' = 'tiezhu_one'
--       ,'scan.startup.mode' = 'earliest-offset'
      ,'scan.startup.mode' = 'latest-offset'
      ,'format' = 'json'
      ,'json.timestamp-format.standard' = 'SQL'
      );

CREATE TABLE sink_one
(
    id        int,
    name      varchar,
    birth     timestamp,
    todayTime time,
    todayDate date
) WITH (
      'connector' = 'cassandra-x',
      'host' = '172.16.100.238,172.16.100.244,172.16.100.67',
      'port' = '9042',
      'user-name' = 'cassandra',
      'password' = 'cassandra',
      'table-name' = 'one',
      'keyspaces' = 'tiezhu'
      );

INSERT INTO sink_one
SELECT id,
       name,
       birth,
       todayTime,
       todayDate
FROM source_one;

