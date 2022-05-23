CREATE TABLE test_kafka
(
    id      int,
    name    VARCHAR,
    message VARCHAR,
    age     int,
    address VARCHAR,
    proc_time AS PROCTIME()
) WITH (
      'connector' = 'stream-x',
      'number-of-rows' = '1000', -- 输入条数，默认无限
      'rows-per-second' = '100' -- 每秒输入条数，默认不限制
      );

CREATE TABLE doris_out_test
(
    id      int,
    name    VARCHAR,
    message VARCHAR,
    age     int,
    address VARCHAR
) WITH (
      'password' = '',
      'connector' = 'doris-x',
      'sink.buffer-flush.interval' = '1000',
      'sink.all-replace' = 'false',
      'sink.buffer-flush.max-rows' = '100',
      'schema' = 'tiezhu',
      'table-name' = 'doris_2',
      'sink.parallelism' = '1',
--       'feNodes' = 'doris_fe:8030',
      'url' = 'jdbc:mysql://doris_fe:9030',
      'username' = 'root'
      );

insert
into doris_out_test
select id      as id,
       name    as name,
       message as message,
       age     as age,
       address as address
from test_kafka;
