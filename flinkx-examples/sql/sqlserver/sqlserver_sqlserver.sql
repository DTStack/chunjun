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
    col_varbinary  varbinary
)with(
   'connector'='sqlserver-x',
   'username'='username',
   'password'='password',
   'url' = 'jdbc:jtds:sqlserver://127.0.0.1:1433;databaseName=db_test;useLOBs=false',
   'schema'='schema',
   'druid.validation-query'='select 1',
   'table-name'='table'
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
    col_varbinary  varbinary

)with(
   'connector'='sqlserver-x',
   'username'='username',
   'password'='password',
   'url' = 'jdbc:jtds:sqlserver://127.0.0.1:1433;databaseName=db_test;useLOBs=false',
   'schema'='schema',
   'table-name'='table'
);
insert into sink
select *
from source;
