-- CREATE TABLE `flink_type` (
--                              `id` int(11) DEFAULT NULL,
--                              `name` varchar(255) DEFAULT NULL,
--                              `money` decimal(9,6) DEFAULT NULL,
--                              `age` bigint(20) DEFAULT NULL,
--                              `datethree` timestamp NULL DEFAULT NULL,
--                              `datesix` timestamp NULL DEFAULT NULL,
--                              `phone` bigint(20) DEFAULT NULL,
--                              `wechat` varchar(255) DEFAULT NULL,
--                              `income` decimal(9,6) DEFAULT NULL,
--                              `birthday` timestamp NULL DEFAULT NULL,
--                              `dtdate` date DEFAULT NULL,
--                              `dttime` time DEFAULT NULL,
--                              `today` date DEFAULT NULL,
--                              `timecurrent` time DEFAULT NULL,
--                              `dateone` timestamp NULL DEFAULT NULL
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
    wechat      varchar,
    income      decimal,
    birthday    timestamp,
    dtdate      date,
    dttime      time,
    today       date,
    timecurrent time
) WITH (
      'connector' = 'mysql-x',
      'url' = 'jdbc:mysql://xxx:3306/test',
      'table-name' = 'flink_type',
      'username' = 'root',
      'password' = 'root'
      ,'scan.fetch-size' = '2'
      ,'scan.query-timeout' = '10'
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
    wechat      varchar,
    income      decimal,
    birthday    timestamp,
    dtdate      date,
    dttime      time,
    today       date,
    timecurrent time
) WITH (
      'connector' = 'stream-x'
      );

insert into sink
select *
from source;
