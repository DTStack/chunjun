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
      ,'scan.startup.mode' = 'earliest-offset'
      -- ,'scan.startup.mode' = 'latest-offset'
      ,'format' = 'json'
      ,'json.timestamp-format.standard' = 'SQL'
      );

-- CREATE TABLE "sink"
-- (
--     "id"             int(11) DEFAULT NULL,
--     "name"           varchar(255)       DEFAULT NULL,
--     "message"        text,
--     "age"            tinyint(4) DEFAULT NULL,
--     "money"          double             DEFAULT NULL,
--     "price"          decimal(10, 0)     DEFAULT NULL,
--     "todayTimestamp" timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
--     "todayDate"      date               DEFAULT NULL,
--     "todayTime"      time               DEFAULT NULL
-- ) ENGINE=EXPRESS DEFAULT CHARSET=utf8 TABLESPACE='sys_tablespace';

-- insert into dev_db.sink (id, name, message, age, money, price, todayTimestamp, todayDate, todayTime)
-- values (1, 'aa', 'bb', 10, 13.2, 33.2, NOW(), NOW(), NOW());

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
