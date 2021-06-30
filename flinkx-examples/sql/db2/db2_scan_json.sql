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
    ID          int,
    NAME        varchar,
    MONEY       decimal,
    DATEONE     timestamp,
    AGE         bigint,
    DATETHREE   timestamp,
    DATESIX     timestamp,
    PHONE       bigint,
    WECHAT      varchar,
    INCOME      decimal,
    BIRTHDAY    timestamp,
    DTDATE      date,
    DTTIME      time,
    TODAY       date,
    TIMECURRENT time,
    ABOOLEAN    smallint ,
    ADOUBLE     double,
    AFLOAT      decimal,
    ACHAR       char,
    ABINARY     BYTES,
    ATINYINT    smallint
) WITH (
      'connector' = 'db2-x',
      'url' = 'jdbc:db2://localtest:50002/DT_TEST',
      'table-name' = 'FLINK_DIM',
      'username' = 'db2inst1',
      'password' = 'dtstack1',
	  'scan.parallelism' = '2',
	  'scan.partition.column' = 'ID'
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
     , s.PHONE
     , s.WECHAT
     , s.INCOME
     , s.BIRTHDAY
     , u.dtdate
     , u.dttime
     , s.TODAY
     , s.TIMECURRENT
     , s.ABOOLEAN
     , s.ADOUBLE
     , s.AFLOAT
     , s.ACHAR
     , s.ABINARY
     , s.ATINYINT
from source u
         left join side s
                   on u.id = s.ID;

insert into sink
select *
from view_out;
