CREATE TABLE source
(
    tag_code  STRING,
    `start`      STRING,
    lastStart     STRING,
    lastStatus   STRING,
    status       STRING
) WITH (
      'connector' = 'redis-x',
      'url'= 'hadoop101:6379',
      'database' = '1',
      'password' = '123456',
      'table-name' = 'run_power*',
      'type' = 'hash',
      'mode' = 'hget'
      );

CREATE TABLE sink
(
    tag_code  STRING,
    `start`      STRING,
    lastStart     STRING,
    lastStatus   STRING,
    status       STRING
) WITH (
      'connector' = 'stream-x',
      'print' = 'true'
      );

insert into sink
select *
from source;
