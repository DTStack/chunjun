-- {"id":100,"name":"lb james阿道夫","money":293.899778,"dateone":"2020-07-30 10:08:22","age":"33","datethree":"2020-07-30 10:08:22.123","datesix":"2020-07-30 10:08:22.123456","datenigth":"2020-07-30 10:08:22.123456789","dtdate":"2020-07-30","dttime":"10:08:22"}
CREATE TABLE source
(
    id        INT,
    name      STRING,
    money     decimal,
    dateone   timestamp,
    age       bigint,
    datethree timestamp,
    datesix   timestamp(6),
    datenigth timestamp(9),
    dtdate    date,
    dttime    time,
    PROCTIME AS PROCTIME()
) WITH (
      'connector' = 'kafka-x'
      ,'topic' = 'da'
      ,'properties.bootstrap.servers' = 'kudu1:9092'
      ,'properties.group.id' = 'luna_g'
      ,'scan.startup.mode' = 'earliest-offset'
      -- ,'scan.startup.mode' = 'latest-offset'
      ,'format' = 'json'
      ,'json.timestamp-format.standard' = 'SQL'
      );

-- CREATE TABLE `flink_out` (
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
--                              `dateone` timestamp NULL DEFAULT NULL,
--                              `aboolean` tinyint(1) DEFAULT NULL,
--                              `adouble` double DEFAULT NULL,
--                              `afloat` float DEFAULT NULL,
--                              `achar` char(1) DEFAULT NULL,
--                              `abinary` binary(1) DEFAULT NULL,
--                              `atinyint` tinyint(4) DEFAULT NULL
-- ) ENGINE=InnoDB DEFAULT CHARSET=utf8
-- INSERT INTO test.flink_out (id, name, money, age, datethree, datesix, phone, wechat, income, birthday, dtdate, dttime, today, timecurrent, dateone) VALUES (100, 'kobe james阿道夫', 30.230000, 30, '2020-03-03 03:03:03', '2020-06-06 06:06:06', 11111111111111, '这是我的wechat', 23.120000, '2020-10-10 10:10:10', '2020-12-12', '12:12:12', '2020-10-10', '10:10:10', '2020-01-01 01:01:01');
-- INSERT INTO test.flink_out (id, name, money, age, datethree, datesix, phone, wechat, income, birthday, dtdate, dttime, today, timecurrent, dateone) VALUES (100, 'kobe james阿道夫', 30.230000, 30, '2020-03-03 03:03:03', '2020-06-06 06:06:06', 11111111111111, '这是我的wechat', 23.120000, '2020-10-10 10:10:10', '2020-12-12', '12:12:12', '2020-10-10', '10:10:10', '2020-01-01 01:01:01');

CREATE TABLE side
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
    timecurrent time,
    aboolean    boolean,
    adouble     double,
    afloat      float,
    achar       char,
    abinary     BYTES,
    atinyint    tinyint,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
      'connector' = 'mysql-x',
      'url' = 'jdbc:mysql://localhost:3306/test',
      'table-name' = 'flink_out',
      'username' = 'root',
      'password' = 'root'

      ,'lookup.cache-type' = 'all' -- 维表缓存类型(NONE、LRU、ALL)，默认：LRU
      ,'lookup.cache-period' = '4600000' -- ALL维表每隔多久加载一次数据，默认：3600000毫秒
      ,'lookup.cache.max-rows' = '20000' -- lru维表缓存数据的条数，默认：10000条
      ,'lookup.cache.ttl' = '700000' -- lru维表缓存数据的时间，默认：60000毫秒
      ,'lookup.fetch-size' = '2000' -- ALL维表每次从数据库加载的条数，默认：1000条
      ,'lookup.async-timeout' = '30000' -- lru维表缓访问超时时间，默认：10000毫秒，暂时没用到
      );


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
    timecurrent time,
    aboolean    boolean,
    adouble     double,
    afloat      float,
    achar       char,
    abinary     BYTES,
    atinyint    tinyint
    , PRIMARY KEY (id) NOT ENFORCED  -- 如果定义了，则根据该字段更新。否则追加
) WITH (
      -- 'connector' = 'stream-x'

      'connector' = 'mysql-x',
      'url' = 'jdbc:mysql://localhost:3306/test',
      'table-name' = 'flink_type',
      'username' = 'root',
      'password' = 'root',

      'sink.buffer-flush.max-rows' = '1024', -- 批量写数据条数，默认：1024
      'sink.buffer-flush.interval' = '10000', -- 批量写时间间隔，默认：10000毫秒
      'sink.all-replace' = 'true', -- 解释如下(其他rdb数据库类似)：默认：false。定义了PRIMARY KEY才有效，否则是追加语句
                                  -- sink.all-replace = 'true' 生成如：INSERT INTO `result3`(`mid`, `mbb`, `sid`, `sbb`) VALUES (?, ?, ?, ?) ON DUPLICATE KEY UPDATE `mid`=VALUES(`mid`), `mbb`=VALUES(`mbb`), `sid`=VALUES(`sid`), `sbb`=VALUES(`sbb`) 。会将所有的数据都替换。
                                  -- sink.all-replace = 'false' 生成如：INSERT INTO `result3`(`mid`, `mbb`, `sid`, `sbb`) VALUES (?, ?, ?, ?) ON DUPLICATE KEY UPDATE `mid`=IFNULL(VALUES(`mid`),`mid`), `mbb`=IFNULL(VALUES(`mbb`),`mbb`), `sid`=IFNULL(VALUES(`sid`),`sid`), `sbb`=IFNULL(VALUES(`sbb`),`sbb`) 。如果新值为null，数据库中的旧值不为null，则不会覆盖。
      'sink.parallelism' = '1'    -- 写入结果的并行度，默认：null
      );

create
TEMPORARY view view_out
  as
select u.id
     , u.name
     , u.money
     , u.dateone
     , u.age
     , u.datethree
     , u.datesix
     , s.phone
     , s.wechat
     , s.income
     , s.birthday
     , u.dtdate
     , u.dttime
     , s.today
     , s.timecurrent
     , s.aboolean
     , s.adouble
     , s.afloat
     , s.achar
     , s.abinary
     , s.atinyint
from source u
         left join side FOR SYSTEM_TIME AS OF u.PROCTIME AS s
                   on u.id = s.id;

insert into sink
select *
from view_out;
