CREATE TABLE source
(
    id        INT,
    name      STRING,
    money     DECIMAL(20, 2),
    dateone   timestamp,
    age       bigint,
    datethree timestamp,
    datesix   timestamp(6),
    dtdate    date
) WITH (
      'connector' = 'stream-x',
      'number-of-rows' = '100', -- 输入条数，默认无限
      'rows-per-second' = '1' -- 每秒输入条数，默认不限制
      );

CREATE TABLE sink
(
    id        INT,
    name      STRING,
    money     DECIMAL(20, 2),
    dateone   timestamp,
    age       bigint,
    datethree timestamp,
    datesix   timestamp(6),
    dtdate    date
) WITH (
    'connector' = 'selectdbcloud-x',
    'host' = '39.105.60.197',
    'http-port' = '59806',
    'query-port' = '28463',
    'cluster-name' = 'test',
    'table.identifier' = 'test.chunjun_test',
    'username' = 'admin',
    'password' = 'SelectDB2023',
    'sink.properties.file.type' = 'json',
    'sink.properties.file.strip_outer_array' = 'true'
);

insert into sink
select *
from source;
