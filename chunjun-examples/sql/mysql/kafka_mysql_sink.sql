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


CREATE TABLE sink
(
    id          int,
    name        varchar,
    money       decimal,
    age         bigint,
    datethree   timestamp,
    datesix     timestamp
) WITH (
       -- 'connector' = 'stream-x'

      'connector' = 'mysql-x',
      'url' = 'jdbc:mysql://localhost:3306/test',
      'table-name' = 'flink_type',
      'username' = 'root',
      'password' = 'abc123',

      'sink.buffer-flush.max-rows' = '1024', -- 批量写数据条数，默认：1024
      'sink.buffer-flush.interval' = '10000', -- 批量写时间间隔，默认：10000毫秒
      'sink.all-replace' = 'true', -- 解释如下(其他rdb数据库类似)：默认：false。定义了PRIMARY KEY才有效，否则是追加语句
                                  -- sink.all-replace = 'true' 生成如：INSERT INTO `result3`(`mid`, `mbb`, `sid`, `sbb`) VALUES (?, ?, ?, ?) ON DUPLICATE KEY UPDATE `mid`=VALUES(`mid`), `mbb`=VALUES(`mbb`), `sid`=VALUES(`sid`), `sbb`=VALUES(`sbb`) 。会将所有的数据都替换。
                                  -- sink.all-replace = 'false' 生成如：INSERT INTO `result3`(`mid`, `mbb`, `sid`, `sbb`) VALUES (?, ?, ?, ?) ON DUPLICATE KEY UPDATE `mid`=IFNULL(VALUES(`mid`),`mid`), `mbb`=IFNULL(VALUES(`mbb`),`mbb`), `sid`=IFNULL(VALUES(`sid`),`sid`), `sbb`=IFNULL(VALUES(`sbb`),`sbb`) 。如果新值为null，数据库中的旧值不为null，则不会覆盖。
      'sink.parallelism' = '1'    -- 写入结果的并行度，默认：null
      );

insert into sink
select id ,name,money,age,datethree,datesix
from source_ods_fact_user_ippv;

