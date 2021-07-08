-- CREATE TABLE films_test (
--     code        char(5),
--     title       varchar(40),
--     did         integer,
--     date_prod   date,
--     kind        varchar(10),
--     len         interval hour to minute,
--     CONSTRAINT production UNIQUE(date_prod)
-- );
--
-- INSERT INTO films_test VALUES ('UA502', 'Bananas', 105,'1971-07-13', 'Comedy', '82 minutes');
-- INSERT INTO films_test VALUES ('UA503', 'Apples', 106,'1971-07-14', 'Fruits', '66 minutes');

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
    t_float double,
    kind varchar
) WITH (
      'connector' = 'kingbase-x',
      'url' = 'jdbc:kingbase8://localhost:54321/MOWEN',
      'schema' = 'public',
      'table-name' = 'type_test',
      'username' = 'SYSTEM',
      'password' = '123456QWE'
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
    t_real     float,
    t_text  varchar,
    t_float double,
    kind varchar
) WITH (
      'connector' = 'kingbase-x',
      'url' = 'jdbc:kingbase8://localhost:54321/MOWEN',
      'schema' = 'public',
      'table-name' = 'type_test_copy2',
      'username' = 'SYSTEM',
      'password' = '123456QWE'
      ,'scan.fetch-size' = '2'
      ,'scan.query-timeout' = '10'
      );

insert into sink
select *
from source;
