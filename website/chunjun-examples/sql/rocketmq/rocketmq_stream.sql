CREATE TABLE rocketmq_source
(
    `id`  BIGINT,
    `name`  STRING,
    `age` INT,
    `boolean_col` BOOLEAN,
    `tinyint_col` TINYINT,
    `smallint_col` SMALLINT,
    `float_col` FLOAT,
    `double_col` DOUBLE,
    `decimal_col` DECIMAL(18,8),
    `char_col` CHAR,
    `time` TIME,
    `date_col` DATE,
    `timestamp_col` TIMESTAMP,
    `byte_col` BINARY
) WITH (
      'connector' = 'rocketmq-x',
      'topic' = 'shitou_test',
      'tag' = 'chunjun-1|chunjun-2',
      'consumer.group' = 'shitou',
      'access.key' = 'RocketMQ',
      'secret.key' = '12345678',
      'nameserver.address' = '127.0.0.1:9876',
      'consumer.start-offset-mode' = 'timestamp', --从指定的时间戳开始消费
      'start.message-offset' = '0',
      'start.time' = '2022-06-15 15:27:18',
      'heartbeat.broker.interval' = '35000',
      'persist.consumer-offset-interval' = '4500',
      'scan.parallelism' = '2' -- 并行度
      );

CREATE TABLE sink
(
    `id`  BIGINT,
    `name`  STRING,
    `age` INT,
    `boolean_col` BOOLEAN,
    `tinyint_col` TINYINT,
    `smallint_col` SMALLINT,
    `float_col` FLOAT,
    `double_col` DOUBLE,
    `decimal_col` DECIMAL(18,8),
    `char_col` CHAR,
    `time` TIME,
    `date_col` DATE,
    `timestamp_col` TIMESTAMP,
    `byte_col` BINARY
) WITH (
      'connector' = 'stream-x',
      'print' = 'true'
      );

insert into sink
select *
from rocketmq_source;



