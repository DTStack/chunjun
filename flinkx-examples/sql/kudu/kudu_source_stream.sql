CREATE TABLE source_one
(
    byte_field   tinyint,
    short_field  smallint,
    int_field    int,
    long_field   bigint,
    binary_field binary,
    string_field string,
    bool_field   boolean,
    float_field  float,
    double_field double,
    PROCTIME AS PROCTIME()
--     unix_time_micros_field unixtime_micros,
--     decimal_field          decimal,
--     varchar_field          varchar,
--     date_field             date
) WITH (
      'connector' = 'kudu-x'
      ,'masters' = '172.16.100.109:7051'
      ,'table-name' = 'tiezhu_test_one'
      );

CREATE TABLE side_one
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
      'url' = 'jdbc:mysql://k3:3306/tiezhu',
      'table-name' = 'flink_out',
      'username' = 'root',
      'password' = 'admin123',
      'lookup.cache-type' = 'lru'
      );

-- CREATE TABLE sink_one
-- (
--     byte_field   byte,
--     short_field  short,
--     int_field    int,
--     long_field   long,
--     binary_field binary,
--     string_field string,
--     bool_field   bool,
--     float_field  float,
--     double_field double
-- ) WITH (
--       'connector' = 'kudu-x'
--       );

CREATE TABLE sink_one
(
    byte_field   tinyint,
    short_field  smallint,
    int_field    int,
    long_field   bigint,
    binary_field binary,
    string_field string,
    bool_field   boolean,
    float_field  float,
    double_field double
) WITH (
      'connector' = 'stream-x'
      );

create
TEMPORARY view view_out
  as
select u.byte_field
     , u.short_field
     , u.int_field
     , u.long_field
     , u.binary_field
     , u.string_field
     , u.bool_field
     , u.float_field
     , u.double_field
from source_one u
         left join side_one FOR SYSTEM_TIME AS OF u.PROCTIME AS s
                   on u.int_field = s.id;

insert into sink_one
select *
from view_out;
