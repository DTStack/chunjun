------------------------------------------------------------------------------------------
CREATE TABLE source (
    id integer primary key,
    col_boolean boolean,
    col_tinyint tinyint,
    col_smallint smallint,
    col_int integer,
    col_bigint bigint,
    col_float float,
    col_double double,
    col_decimal decimal(20,4),
    col_string varchar,
    col_varchar varchar(255),
    col_char char(255),
    col_timestamp timestamp,
    col_date date
)  WITH (
    'connector' = 'phoenix5-x',
    'username' = '',
    'password' = '',
    'url' = 'jdbc:phoenix:chunjun1,chunjun2,chunjun3:2181',
    'table-name' = 'source'
);

-- 'cat' = 'insert,delete,update',
-- 'timestamp-format.standard' = 'SQL'
------------------------------------------------------------------------------------------
CREATE TABLE sink (
    id integer primary key,
    col_boolean boolean,
    col_tinyint tinyint,
    col_smallint smallint,
    col_int integer,
    col_bigint bigint,
    col_float float,
    col_double double,
    col_decimal decimal(20,4),
    col_string varchar,
    col_varchar varchar(255),
    col_char char(255),
    col_timestamp timestamp,
    col_date date
) WITH ('connector' = 'stream-x');
------------------------------------------------------------------------------------------
insert into sink
select *
from source u;
