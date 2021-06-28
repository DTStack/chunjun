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
    aboolean    smallint ,
    adouble     double,
    afloat      decimal,
    achar       char,
    abinary     BYTES,
    atinyint    smallint
) WITH (
      'connector' = 'db2-x',
      'url' = 'jdbc:db2://172.16.101.246:50002/DT_TEST',
      'table-name' = 'flink_dim',
      'username' = 'db2inst1',
      'password' = 'dtstack1',
	  'scan.parallelism' = '2',
	  'scan.partition.column' = 'id'
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
    aboolean    smallint ,
    adouble     double,
    afloat      decimal ,
    achar       char,
    abinary     BYTES,
    atinyint    smallint ,
	PRIMARY KEY(id) NOT ENFORCED
) WITH (
      'connector' = 'stream-x',
	  'sink.parallelism' = '1'
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
         left join side s
                   on u.id = s.id;

insert into sink
select *
from view_out;
