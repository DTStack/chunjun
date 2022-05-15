-- {"id":100,"name":"lb james阿道夫","money":293.899778,"dateone":"2020-07-30 10:08:22","age":"33","datethree":"2020-07-30 10:08:22.123","datesix":"2020-07-30 10:08:22.123456","datenigth":"2020-07-30 10:08:22.123456789","dtdate":"2020-07-30","dttime":"10:08:22"}
CREATE TABLE source_ods_fact_user_ippv
(
    id           INT,
    name         STRING,
    money        decimal,
    dateone      timestamp,
    age          bigint,
    datethree    timestamp,
    datesix      timestamp(6),
    datenigth    timestamp(9),
    dtdate       date,
    dttime       time,

    ts           TIMESTAMP(3) METADATA FROM 'timestamp'
    ,
    partition_id BIGINT METADATA FROM 'partition' VIRTUAL -- from Kafka connector
    ,
    WATERMARK FOR datethree AS datethree - INTERVAL '5' SECOND
) WITH (
      'connector' = 'kafka-x'
      ,'topic' = 'da'
      ,'properties.bootstrap.servers' = 'localhost:9092'
      ,'properties.group.id' = 'luna_g'
      ,'scan.startup.mode' = 'earliest-offset'
      -- ,'scan.startup.mode' = 'latest-offset'
      ,'format' = 'json'
      ,'json.timestamp-format.standard' = 'SQL'
      -- ,'scan.parallelism' = '3'
      );


CREATE TABLE result_total_pvuv_min
(
    id           INT,
    name         STRING,
    money        decimal,
    dateone      timestamp,
    age          bigint,
    datethree    timestamp,
    datesix      timestamp(6),
    datenigth    timestamp(9),
    dtdate       date,
    dttime       time,

    ts           TIMESTAMP(3)
    ,
    partition_id BIGINT
    ,
    PRIMARY KEY (id, name, money, dateone, age, datethree, datesix, datenigth, dtdate, dttime, ts,
                 partition_id) NOT ENFORCED
) WITH (
      -- 'connector' = 'stream-x'

      'connector' = 'upsert-kafka-x'

      ,'topic' = 'test'
      ,'properties.bootstrap.servers' = 'localhost:9092'
      ,'key.format' = 'json'
      ,'value.format' = 'json'
      ,'value.fields-include' = 'ALL'
      -- ,'sink.parallelism' = '2'
      );


INSERT INTO result_total_pvuv_min
SELECT id
     , name
     , money
     , dateone
     , age
     , datethree
     , datesix
     , datenigth
     , dtdate
     , dttime
     , ts
     , partition_id
from source_ods_fact_user_ippv
group by id
       , name
       , money
       , dateone
       , age
       , datethree
       , datesix
       , datenigth
       , dtdate
       , dttime
       , ts
       , partition_id
;
