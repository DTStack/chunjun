--complex with flink original kafka-connector
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

CREATE TABLE source_ods_fact_user_original (
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
 'connector' = 'kafka',
  'topic' = 'user_behavior',
  'properties.bootstrap.servers' = 'localhost:9092',
  'properties.group.id' = 'testGroup',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json'
      );


CREATE TABLE result_total_pvuv_min
(
    id INT
    , name STRING
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
SELECT
a.id,b.name
from source_ods_fact_user_ippv a
left join
source_ods_fact_user_original b
on a.id = b.id;

