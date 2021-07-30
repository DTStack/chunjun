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
      'connector' = 'mysql-x',
      'url' = 'jdbc:mysql://localhost:3306/test',
      'table-name' = 'flink_type',
      'username' = 'root',
      'password' = 'root'

      ,'scan.parallelism' = '2' -- 并行度大于1时，必须指定scan.partition.column。默认：1
      ,'scan.fetch-size' = '2' -- 每次从数据库中fetch大小。默认：1024条
      ,'scan.query-timeout' = '10' -- 数据库连接超时时间。默认：不超时

      ,'scan.partition.column' = 'id' -- 多并行度读取的切分字段，多并行度下必需要设置。无默认
      ,'scan.partition.strategy' = 'range' -- 数据分片策略。默认：range，如果并行度大于1，且是增量任务或者间隔轮询，则会使用mod分片

      -- ,'scan.increment.column' = 'id' -- 增量字段名称，必须是表中字段。非必填，无默认
      -- ,'scan.increment.column-type' = 'int' -- 增量字段类型。非必填，无默认
      -- ,'scan.start-location' = '109' -- 增量字段开始位置。非必填，无默认，如果没配置scan.increment.column，则不生效

      ,'scan.restore.columnname' = 'id' -- 开启了cp，任务从sp/cp续跑字段名称。如果续跑，则会覆盖scan.start-location开始位置，从续跑点开始。非必填，无默认
      ,'scan.restore.columntype' = 'int' -- 开启了cp，任务从sp/cp续跑字段类型。非必填，无默认
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
