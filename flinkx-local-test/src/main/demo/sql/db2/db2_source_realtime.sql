
CREATE TABLE source
(
    ID          int,
    NAME        varchar,
    MONEY       decimal,
    DATEONE     timestamp,
    AGE         bigint,
    DATETHREE   timestamp,
    DATESIX     timestamp,
    PHONE       bigint,
    WECHAT      STRING,
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
    ATINYINT    smallint
) WITH (
      'connector' = 'db2-x',
      'url' = 'jdbc:db2://localtest:50002/DT_TEST',
      'table-name' = 'FLINK_DIM',
      'username' = 'db2inst1',
      'password' = 'dtstack1'

      ,'scan.parallelism' = '1' -- 间隔轮训不支持多并行度
      ,'scan.partition.column' = 'ID' -- 多并行度读取的切分字段

      ,'scan.increment.column' = 'ID' -- 增量字段
      ,'scan.increment.column-type' = 'int'  -- 增量字段类型
      ,'scan.start-location' = '1' --增量字段开始位置,如果不指定则先查询所有并查询scan.increment.column最大值作为下次起始位置

      ,'scan.fetch-size' = '2' -- fetch抓取的条数,防止一次抓取太多
      ,'scan.query-timeout' = '10' -- 数据库抓取超时时间
      ,'scan.polling-interval' = '3000' --间隔轮训时间

      ,'scan.restore.columnname' = 'ID' -- 续跑字段
      ,'scan.restore.columntype' = 'int' -- 续跑字段类型
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
    aboolean    smallint ,
    adouble     double,
    afloat      double ,
    achar       char,
    abinary     BYTES,
    atinyint    smallint
) WITH (
      'connector' = 'stream-x'
      );

insert into sink
select *
from source;

