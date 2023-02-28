CREATE TABLE source
(
    `timestamp` TIMESTAMP,
    `type` INT,
    `error_code` INT,
    `error_msg` VARCHAR(1024),
    `op_id` BIGINT,
    `op_time` TIMESTAMP
) WITH (
      'connector' = 'stream-x',
      'number-of-rows' = '100', -- 输入条数，默认无限
      'rows-per-second' = '1' -- 每秒输入条数，默认不限制
      );

CREATE TABLE sink
(
    `timestamp` TIMESTAMP,
    `type` INT,
    `error_code` INT,
    `error_msg` VARCHAR(1024),
    `op_id` BIGINT,
    `op_time` TIMESTAMP
) WITH (
    'connector' = 'selectdbcloud-x',
    'host' = 'xxx',
    'http-port' = '46635',
    'query-port' = '12634',
    'cluster-name' = 'test',
    'table.identifier' = 'test.dup_tbl',
    'username' = 'admin',
    'password' = 'xxx',
    'sink.properties.file.type' = 'json',
    'sink.properties.file.strip_outer_array' = 'true',
    'sink.enable-delete' = 'false'
);

insert into sink
select *
from source;
