CREATE TABLE source
(
    id             int,
    name           varchar,
    price          decimal,
    money          double,
    message        string,
    age            tinyint,
    todayTimestamp timestamp,
    todayDate      date,
    todayTime      time
) WITH (
      'connector' = 'gbase-x',
      'url' = 'jdbc:gbase://gbase:5258/dev_db',
      'table-name' = 'sink',
      'schema' = 'dev_db',
      'username' = 'dev',
      'password' = 'dev123',
      'scan.fetch-size' = '2',
      'scan.query-timeout' = '10'
      );

CREATE TABLE sink
(
    id             int,
    name           varchar,
    price          decimal,
    money          double,
    message        string,
    age            tinyint,
    todayTimestamp timestamp,
    todayDate      date,
    todayTime      time
) WITH (
      'connector' = 'stream-x'
      );

insert into sink
select *
from source;
