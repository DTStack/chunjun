-- {"id":100,"name":"lb james阿道夫","money":293.899778,"dateone":"2020-07-30 10:08:22","age":"33","datethree":"2020-07-30 10:08:22.123","datesix":"2020-07-30 10:08:22.123456","datenigth":"2020-07-30 10:08:22.123456789","dtdate":"2020-07-30","dttime":"10:08:22"}
CREATE TABLE source_ods_fact_user_ippv (
    id INT
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

    , WATERMARK FOR datethree AS datethree - INTERVAL '5' SECOND
) WITH (
      'connector' = 'kafka-x'
      ,'topic' = 'user_behavior'
      ,'properties.bootstrap.servers' = 'localhost:9092'
      ,'properties.group.id' = 'luna_g'
      ,'scan.startup.mode' = 'earliest-offset'
      ,'format' = 'json'
      ,'json.timestamp-format.standard' = 'SQL'
      ,'scan.parallelism' = '1'
      );


CREATE TABLE result_total_pvuv_min
(
    id INT
    , name STRING
    , money decimal
    , dateone timestamp
    , age bigint
    , datethree timestamp
    , datesix timestamp(6)
    , datenigth timestamp(9)
    , dtdate date
    , dttime time


    , `partition` BIGINT
    , `topic` STRING
    , `leader-epoch` int
    , `offset` BIGINT
    , ts TIMESTAMP(3)
    , `timestamp-type` STRING
    , partition_id BIGINT
) WITH (
      'connector' = 'stream-x'

      -- 'connector' = 'kafka-x'
      -- ,'topic' = 'test'
      -- ,'properties.bootstrap.servers' = 'localhost:9092'
      -- ,'format' = 'json'
      -- ,'sink.parallelism' = '2'
      -- ,'json.timestamp-format.standard' = 'SQL'
      );


INSERT INTO result_total_pvuv_min
SELECT *
from source_ods_fact_user_ippv;
