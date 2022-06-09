-- create table type_test (
--     code char(5),
--     title varchar(40),
--     did integer,
--     t_bigint  bigint,
--     t_smallint smallint,
--     data_prod date,
--     timestamp_prod timestamp,
--     time_prod time,
--     t_double double precision,
--     t_numeric numeric(10,5),
--     t_decimal decimal(10,5),
--     t_tinyint tinyint,
--     t_real     real,
--     t_text  text,
--     t_float float,
--     kind varchar(10)
-- );
--
-- INSERT INTO type_test
-- VALUES ('UA503',
--         'Bananas',
--         105,
--         88888888,
--         2,
--         '1971-07-13',
--         CAST('2021-05-18 20:36:34' AS TIMESTAMP),
--         '20:36:34',
--         0.288888,
--         0.555,
--         13.66,
--         1,
--         0.9,
--         'text',
--         0.1,
--         'Comedy');
--

CREATE TABLE source
(
    code char,
    title varchar,
    did integer,
    t_bigint  bigint,
    t_smallint smallint,
    data_prod date,
    timestamp_prod timestamp,
    time_prod time,
    t_double double precision,
    t_numeric numeric,
    t_decimal decimal,
    t_tinyint tinyint,
    t_real     float,
    t_text  varchar,
    t_float double
) WITH (
      'connector' = 'kingbase-x',
      'url' = 'jdbc:kingbase8://localhost:54321/MOWEN',
--       'schema' = 'mowen',
      'table-name' = 'type_test1',
      'username' = 'SYSTEM',
      'password' = '123456QWE'

      ,'scan.parallelism' = '1' -- 间隔轮训不支持多并行度
      ,'scan.partition.column' = 'did' -- 多并行度读取的切分字段

      ,'scan.increment.column' = 'did' -- 增量字段
      ,'scan.increment.column-type' = 'int'  -- 增量字段类型
      ,'scan.start-location' = '88' --增量字段开始位置,如果不指定则先查询所有并查询scan.increment.column最大值作为下次起始位置

      ,'scan.fetch-size' = '2' -- fetch抓取的条数,防止一次抓取太多
      ,'scan.query-timeout' = '10' -- 数据库抓取超时时间
      ,'scan.polling-interval' = '3000' --间隔轮训时间

      ,'scan.restore.columnname' = 'did' -- 续跑字段
      ,'scan.restore.columntype' = 'int' -- 续跑字段类型
      );

CREATE TABLE sink
(
    code char,
    title varchar,
    did integer,
    t_bigint  bigint,
    t_smallint smallint,
    data_prod date,
    timestamp_prod timestamp,
    time_prod time,
    t_double double precision,
    t_numeric numeric,
    t_decimal decimal,
    t_tinyint tinyint,
    t_real     float,
    t_text  varchar,
    t_float double
) WITH (
      'connector' = 'stream-x'
      );

insert into sink
select *
from source;
