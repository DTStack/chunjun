-- CREATE TABLE `flink_type` (
--                               `id` int(11) DEFAULT NULL,
--                               `name` varchar(255) DEFAULT NULL,
--                               `money` decimal(9,6) DEFAULT NULL,
--                               `age` bigint(20) DEFAULT NULL,
--                               `datethree` timestamp NULL DEFAULT NULL,
--                               `datesix` timestamp NULL DEFAULT NULL,
--                               `phone` bigint(20) DEFAULT NULL,
--                               `wechat` varchar(255) DEFAULT NULL,
--                               `income` decimal(9,6) DEFAULT NULL,
--                               `birthday` timestamp NULL DEFAULT NULL,
--                               `dtdate` date DEFAULT NULL,
--                               `dttime` time DEFAULT NULL,
--                               `today` date DEFAULT NULL,
--                               `timecurrent` time DEFAULT NULL,
--                               `dateone` timestamp NULL DEFAULT NULL,
--                               `aboolean` tinyint(1) DEFAULT '1',
--                               `adouble` double DEFAULT '123.134',
--                               `afloat` float DEFAULT '23.4',
--                               `achar` char(1) DEFAULT 'a',
--                               `abinary` binary(1) DEFAULT '1',
--                               `atinyint` tinyint(4) DEFAULT '12'
-- ) ENGINE=InnoDB DEFAULT CHARSET=utf8
-- INSERT INTO test.flink_type (id, name, money, age, datethree, datesix, phone, wechat, income, birthday, dtdate, dttime, today, timecurrent, dateone) VALUES (100, 'kobe james阿道夫', 30.230000, 30, '2020-03-03 03:03:03', '2020-06-06 06:06:06', 11111111111111, '这是我的wechat', 23.120000, '2020-10-10 10:10:10', '2020-12-12', '12:12:12', '2020-10-10', '10:10:10', '2020-01-01 01:01:01');
-- INSERT INTO test.flink_type (id, name, money, age, datethree, datesix, phone, wechat, income, birthday, dtdate, dttime, today, timecurrent, dateone) VALUES (100, 'kobe james阿道夫', 30.230000, 30, '2020-03-03 03:03:03', '2020-06-06 06:06:06', 11111111111111, '这是我的wechat', 23.120000, '2020-10-10 10:10:10', '2020-12-12', '12:12:12', '2020-10-10', '10:10:10', '2020-01-01 01:01:01');


CREATE TABLE source
(
    id          int,
    name        varchar,
    money       decimal,
    dateone     timestamp,
    age         bigint,
    datethree   timestamp,
    datesix     timestamp,
    phone       bigint,
    wechat      STRING,
    income      decimal,
    birthday    timestamp,
    dtdate      date,
    dttime      time,
    today       date,
    timecurrent time,
    aboolean    boolean,
    adouble     double,
    afloat      float,
    achar       char,
    abinary     BYTES,
    atinyint    tinyint
) WITH (
      'connector' = 'db2-x',
      'url' = 'jdbc:mysql://xxx:3306/test',
      'table-name' = 'flink_type',
      'username' = 'root',
      'password' = 'root'

      ,'scan.parallelism' = '2' -- 并行度大于1时，必须指定scan.partition.column
      ,'scan.fetch-size' = '2'
      ,'scan.query-timeout' = '10'

      ,'scan.partition.column' = 'id' -- 多并行度读取的切分字段

      ,'scan.increment.column' = 'id' -- 增量字段
      ,'scan.increment.column-type' = 'int' -- 增量字段类型
      ,'scan.start-location' = '109' --增量字段开始位置
      );

CREATE TABLE sink
(
    id          int,
    name        varchar,
    money       decimal,
    dateone     timestamp,
    age         bigint,
    datethree   timestamp,
    datesix     timestamp,
    phone       bigint,
    wechat      STRING,
    income      decimal,
    birthday    timestamp,
    dtdate      date,
    dttime      time,
    today       date,
    timecurrent time,
    aboolean    boolean,
    adouble     double,
    afloat      float,
    achar       char,
    abinary     BYTES,
    atinyint    tinyint
) WITH (
      'connector' = 'stream-x'
      );

insert into sink
select *
from source;
