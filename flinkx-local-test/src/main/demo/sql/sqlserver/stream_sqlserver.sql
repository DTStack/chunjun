CREATE TABLE source
(
    id             bigint,
    col_bit        BOOLEAN,
    col_tinyint    tinyint,
    col_smallint   int,
    col_int        int,
    col_real       float,
    col_float      double,
    col_decimal    decimal(10, 3),
    col_numric     decimal(10, 3),
    col_char       char(10),
    col_varchar    varchar(255),
    col_varcharmax string,
--     col_nochar      string,
--     col_novarchar   string,
--     col_notext      string,
    col_date       date,
    col_time       time,
--    col_datetime   timestamp,
--    col_datetime2  string,
--    col_smalldatetime  timestamp,
--    col_timestamp  bytes,
--    col_text       string,
--    col_xml        string,
--    col_binary     bytes,
    col_varbinary  varbinary
)with(
    'connector'='stream-x'
);

CREATE TABLE sink
(
    id             bigint,
    col_bit        BOOLEAN,
    col_tinyint    tinyint,
    col_smallint   int,
    col_int        int,
    col_real       float,
    col_float      double,
    col_decimal    decimal(10, 3),
    col_numric     decimal(10, 3),
    col_char       char(10),
    col_varchar    varchar(255),
    col_varcharmax string,
--     col_nochar      string,
--     col_novarchar   string,
--     col_notext      string,
    col_date       date,
    col_time       time,
--    col_datetime   timestamp,
--    col_datetime2  string,
--    col_smalldatetime  timestamp,
--    col_timestamp  bytes,
--    col_text       string,
--    col_xml        string,
--    col_binary     bytes,
    col_varbinary  varbinary

)with(
   'connector'='SqlServer-x',
   'username'='username',
   'password'='password',
   'url' = 'jdbc:jtds:sqlserver://localhost:1433;databaseName=db_test;useLOBs=false',
   'schema'='schema',
   'table-name'='table'
);
insert into sink
select *
from source;
