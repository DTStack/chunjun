CREATE TABLE source
(
    id             bigint,
    col_bit        BOOLEAN,
    col_tinyint    tinyint,
    col_smallint   smallint,
    col_int        int,
    col_real       float,
    col_float      double,
    col_decimal    decimal(10, 3),
    col_numric     decimal(10, 3),
    col_char       char(10),
    col_varchar    varchar(255),
    col_varcharmax string,
    col_date       date,
    col_time       string,
    col_timestamp  bytes,
    col_varbinary  varbinary,
    PROCTIME AS PROCTIME()
)with(
   'connector'='sqlserver-x',
   'username'='username',
   'password'='password',
   'url' = 'jdbc:jtds:sqlserver://127.0.0.1:1433;databaseName=db_test;useLOBs=false',
   'schema'='schema',
   'druid.validation-query'='select 1',
   'table-name'='table'
);

CREATE TABLE side
(
    id             bigint,
    col_bit        BOOLEAN,
    col_tinyint    tinyint,
    col_smallint   smallint,
    col_int        int,
    col_real       float,
    col_float      double,
    col_decimal    decimal(10, 3),
    col_numric     decimal(10, 3),
    col_char       char(10),
    col_varchar    varchar(255),
    col_varcharmax string,
    col_date       date,
    col_time       string,
    col_timestamp  bytes,
    col_varbinary  varbinary,
    PRIMARY KEY (id) NOT ENFORCED
)with(
   'connector'='sqlserver-x',
   'username'='username',
   'password'='password',
   'url' = 'jdbc:jtds:sqlserver://127.0.0.1:1433;databaseName=db_test;useLOBs=false',
   'schema'='schema',
   'table-name'='table',
   'druid.validation-query'='select 1',
   'lookup.cache-type' = 'lru'
);

CREATE TABLE sink
(
    id             bigint,
    col_bit        BOOLEAN,
    col_tinyint    tinyint,
    col_smallint   smallint,
    col_int        int,
    col_real       float,
    col_float      double,
    col_decimal    decimal(10, 3),
    col_numric     decimal(10, 3),
    col_char       char(10),
    col_varchar    varchar(255),
    col_varcharmax string,
    col_date       date,
    col_time       string,
    col_varbinary  varbinary,
    PROCTIME AS PROCTIME()
)with(
   'connector'='sqlserver-x',
   'username'='username',
   'password'='password',
   'url' = 'jdbc:jtds:sqlserver://127.0.0.1:1433;databaseName=db_test;useLOBs=false',
   'schema'='schema',
   'table-name'='table',
   'sink.buffer-flush.max-rows' = '1',
   'sink.all-replace' = 'true'
);

create
TEMPORARY view view_out
  as
select u.id
     , u.col_bit
     , u.col_tinyint
     , u.col_smallint
     , u.col_int
     , u.col_real
     , u.col_float
     , u.col_decimal
     , u.col_numric
     , s.col_char
     , s.col_varchar
     , s.col_varcharmax
     , s.col_date
     , s.col_time
     , u.col_varchar
from source u
         left join side FOR SYSTEM_TIME AS OF u.PROCTIME AS s
                   on u.id = s.id;

insert into sink
select *
from view_out;
