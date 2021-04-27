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
    t_real     real,
    t_text  text,
    t_float float,
    kind varchar
) WITH (
      'connector' = 'kingbase-x',
      'url' = 'jdbc:kingbase8://localhost:54321/MOWEN',
--       'schema' = 'mowen',
      'table-name' = 'films_test',
      'username' = 'SYSTEM',
      'password' = '123456QWE'
      ,'scan.partition.column' = 'code'
      ,'scan.polling-interval' = '3000'
      ,'scan.start-location' = '20'
      ,'scan.fetch-size' = '2'
      ,'scan.query-timeout' = '10'
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
    t_real     real,
    t_text  text,
    t_float float,
    kind varchar
) WITH (
      'connector' = 'stream-x'
      );

insert into sink
select *
from source;
