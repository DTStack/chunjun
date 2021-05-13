CREATE TABLE source
(
    id   INT,
    name STRING,
    age  tinyint,
    PROCTIME AS PROCTIME()
) WITH (
      'connector' = 'kafka-x'
      ,'topic' = 'tiezhu_in_one'
      ,'properties.bootstrap.servers' = 'kudu1:9092'
      ,'properties.group.id' = 'luna_g'
      ,'scan.startup.mode' = 'latest-offset'
      -- ,'scan.startup.mode' = 'latest-offset'
      ,'format' = 'json'
      ,'json.timestamp-format.standard' = 'SQL'
      );

CREATE TABLE side
(
    id             int,
    name           varchar,
    price          decimal,
    money          double,
    message        string,
    age            tinyint,
    todayTimestamp timestamp,
    todayDate      date,
    todayTime      time,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
      'connector' = 'gbase-x',
      'url' = 'jdbc:gbase://gbase:5258/dev_db',
      'table-name' = 'sink',
      'schema' = 'dev_db',
      'username' = 'dev',
      'password' = 'dev123',
--       'lookup.cache-type' = 'lru'
      'lookup.cache-type' = 'all'
      );

CREATE TABLE sink
(
    id             int,
    name           varchar,
    price          decimal,
    money          double,
    message        string,
    age            tinyint,
    todayDate      date,
    todayTime      time,
    todayTimestamp timestamp
) WITH (
      'connector' = 'stream-x'
      );

create
TEMPORARY view view_out
  as
select u.id             AS id,
       u.name           AS name,
       s.price          AS price,
       s.money          AS money,
       s.name           AS message,
       u.age            AS age,
       s.todayDate      AS todayDate,
       s.todayTime      AS todayTime,
       s.todayTimestamp AS todayTimestamp
from source u
         left join side FOR SYSTEM_TIME AS OF u.PROCTIME AS s
                   on u.id = s.id;

insert into sink
select *
from view_out;
