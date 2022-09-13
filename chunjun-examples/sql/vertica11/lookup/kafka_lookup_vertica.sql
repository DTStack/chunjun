-- {"id":100,"name":"lb james阿道夫","money":293.899778,"dateone":"2020-07-30 10:08:22","age":"33","datethree":"2020-07-30 10:08:22.123","datesix":"2020-07-30 10:08:22.123456","datenigth":"2020-07-30 10:08:22.123456789","dtdate":"2020-07-30","dttime":"10:08:22"}
CREATE TABLE source_ods_fact_user_ippv (
    id int
    , name STRING
    , money decimal
    , dateone timestamp
    , age bigint
    , datethree timestamp
    , datesix timestamp(6)
    , datenigth timestamp(9)
    , dtdate date
    , dttime time
    , `partition` BIGINT METADATA VIRTUAL -- from Kafka connector
    , `topic` STRING METADATA VIRTUAL -- from Kafka connector
    , `leader-epoch` int METADATA VIRTUAL -- from Kafka connector
    , `offset` BIGINT METADATA VIRTUAL  -- from Kafka connector
    , ts TIMESTAMP(3) METADATA FROM 'timestamp' -- from Kafka connector
    , `timestamp-type` STRING METADATA VIRTUAL  -- from Kafka connector
    , partition_id BIGINT METADATA FROM 'partition' VIRTUAL   -- from Kafka connector
    , WATERMARK FOR datethree AS datethree - INTERVAL '5' SECOND,
    PROCTIME AS PROCTIME()
) WITH (
      'connector' = 'kafka-x'
      ,'topic' = 'user_behavior'
      ,'properties.bootstrap.servers' = 'localhost:9092'
      ,'properties.group.id' = 'luna_g'
      ,'scan.startup.mode' = 'latest-offset'
      ,'format' = 'json'
      ,'json.timestamp-format.standard' = 'SQL'
      ,'scan.parallelism' = '1'
      );

CREATE TABLE side
(
     varchar1 varchar,
--     varbinary1 varbinary,
--     boolean1 boolean,
--     --binary1 binary,
--     char1 char,
--     float1 float,
--     float2 float,
    integer1 bigint
--     numeric1 numeric,
--     time1 time,
--     timestamp1 timestamp,
--     long_varchar1 varchar,
--     long_varbinary varbinary,
--     date1 date
) WITH (
      'connector' = 'vertica11-x',
      'url' = 'jdbc:vertica://localhost:5433/',
      'table-name' = 'menghantest.test_type2',
      'username' = 'dbadmin',
      'password' = '',
      'lookup.cache-type' = 'lru'
--       'lookup.cache-type' = 'all'
      );

CREATE TABLE sink
(
    id          int,
    name     varchar
    --PRIMARY KEY(varchar1) NOT ENFORCED
) WITH (
      'connector' = 'vertica11-x',
      'url' = 'jdbc:vertica://localhost:5433/',
      'table-name' = 'menghantest.test2',
      'username' = 'dbadmin',
      'password' = '',
      'sink.buffer-flush.max-rows' = '2000',
      'sink.all-replace' = 'true',
      'sink.buffer-flush.interval' = '10'
      );

insert into sink
select t1.id as id,t2.varchar1 as name
from source_ods_fact_user_ippv t1
left join side FOR SYSTEM_TIME AS OF t1.PROCTIME AS t2 on t1.id = t2.integer1;

