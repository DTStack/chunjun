CREATE TABLE test_kafka
(
    id      int,
    name    VARCHAR
) WITH (
      'connector' = 'stream-x',
      'number-of-rows' = '1000', -- 输入条数，默认无限
      'rows-per-second' = '100' -- 每秒输入条数，默认不限制
      );

CREATE TABLE doris_out_test
(
    id      int,
    name    VARCHAR
) WITH (
      'password' = '',
      'connector' = 'doris-x',
      'sink.buffer-flush.interval' = '1000',
      'sink.all-replace' = 'false',
      'sink.buffer-flush.max-rows' = '100',
      'schema' = 'test_1121',
      'table-name' = 'test_002',
      'sink.parallelism' = '1',
      'feNodes' = '172.16.124.70:18030',
--       'url' = 'jdbc:mysql://doris_fe:9030',
      'username' = 'root'
      );

insert
into doris_out_test
select id      as id,
       name    as name
from test_kafka;
