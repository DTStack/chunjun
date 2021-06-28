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
    AFLOAT      decimal ,
    ACHAR       char,
    ABINARY     BYTES,
    ATINYINT    smallint ,
    PRIMARY KEY (ID) NOT ENFORCED
) WITH (
      'connector' = 'db2-x',
      'url' = 'jdbc:db2://localtest:50002/DT_TEST',
      'table-name' = 'FLINK_DIM',
      'username' = 'db2inst1',
      'password' = 'dtstack1',
      'lookup.cache-type' = 'all',
	  'lookup.parallelism' = '2'
      );

CREATE TABLE sink
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
    ABOOLEAN    smallint,
    ADOUBLE     double,
    AFLOAT      decimal ,
    ACHAR       char,
    ABINARY     BYTES,
    ATINYINT    smallint
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
         left join side FOR SYSTEM_TIME AS OF u.PROCTIME AS s
                   on u.id = s.ID;

insert into sink
select *
from view_out;
