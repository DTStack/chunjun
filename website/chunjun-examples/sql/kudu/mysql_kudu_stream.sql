CREATE TABLE source_one
(
    id int,
    PROCTIME AS PROCTIME()
) WITH (
      'connector' = 'mysql-x'
      ,'url' = 'jdbc:mysql://k3:3306/tiezhu'
      ,'table-name' = 'flink_type'
      ,'username' = 'root'
      ,'password' = 'admin123'
      );

CREATE TABLE side_one
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
--     unix_time_micros_field unixtime_micros,
--     decimal_field          decimal,
--     varchar_field          varchar,
--     date_field             date,
    PRIMARY KEY (int_field) NOT ENFORCED
) WITH (
      'connector' = 'kudu-x',
      'masters' = 'eng-cdh1:7051',
      'table-name' = 'tiezhu_test_two',
      'keytab' = '/Users/wtz/dtstack/conf/kerberos/eng-cdh/hive3.keytab',
      'principal' = 'hive/eng-cdh3@DTSTACK.COM',
      'krb5conf' = '/Users/wtz/dtstack/conf/kerberos/eng-cdh/krb5.conf',
      'lookup.cache-type' = 'lru'
--       'lookup.cache-type' = 'all'
      );

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
select s.byte_field
     , s.short_field
     , s.int_field
     , s.long_field
     , s.binary_field
     , s.string_field
     , s.bool_field
     , s.float_field
     , s.double_field
from source_one u
         join side_one FOR SYSTEM_TIME AS OF u.PROCTIME AS s
              on u.id = s.int_field;

insert into sink_one
select *
from view_out;
