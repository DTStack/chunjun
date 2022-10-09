-- {"id":100,"name":"lb james阿道夫","money":293.899778,"dateone":"2020-07-30 10:08:22","age":"33","datethree":"2020-07-30 10:08:22.123","datesix":"2020-07-30 10:08:22.123456","datenigth":"2020-07-30 10:08:22.123456789","dtdate":"2020-07-30","dttime":"10:08:22"}
CREATE TABLE source_ods_fact_user_ippv (
    id INT
    , name STRING
    , money double
    , dateone timestamp
    , age bigint
    , datethree timestamp(6)
    , datesix timestamp(6)
    , datenigth timestamp(6)
    , dtdate date
    , dttime time
    , `partition` BIGINT METADATA VIRTUAL -- from Kafka connector
    , `topic` STRING METADATA VIRTUAL -- from Kafka connector
    , `leader-epoch` int METADATA VIRTUAL -- from Kafka connector
    , `offset` BIGINT METADATA VIRTUAL  -- from Kafka connector
    , ts TIMESTAMP(3) METADATA FROM 'timestamp' -- from Kafka connector
    , `timestamp-type` STRING METADATA VIRTUAL  -- from Kafka connector
    , partition_id BIGINT METADATA FROM 'partition' VIRTUAL   -- from Kafka connector

    , WATERMARK FOR datethree AS datethree - INTERVAL '5' SECOND
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


CREATE TABLE sink
(
    int1 int
    ,varchar1 STRING
    , numeric1 double
    , timestamp1 timestamp
    , integer1 bigint
    ,timestamp2 timestamp
    , timestamp6 timestamp
    , timestamp9 timestamp
    , date1 date
    , time1 time
    --PRIMARY KEY(varchar1) NOT ENFORCED
) WITH (
      'connector' = 'vertica11-x',
      'url' = 'jdbc:vertica://localhost:5433/',
      'table-name' = 'menghantest.test_type2',
      'username' = 'dbadmin',
      'password' = '',
      'sink.buffer-flush.max-rows' = '2000',
      'sink.all-replace' = 'true',
      'sink.buffer-flush.interval' = '10'
      );

insert into sink
select id as int1,name as varchar1,money as numeric1,dateone as timestamp1,age as integer1,datethree as timestamp2,datesix as varchar1,
datenigth as timestamp9,dtdate as date1,dttime as time1
from source_ods_fact_user_ippv;

