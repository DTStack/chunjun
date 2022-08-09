CREATE TABLE source
(
    id             bigint,
    col_bit        BOOLEAN,
    col_tinyint    tinyint,
    col_smallint   int,
    col_mediumint  int,
    col_int        int,
    col_integer    int,
    col_bigint     bigint,
    col_real       double,
    col_float      float,
    col_decimal    decimal(10, 0),
    col_numric     decimal(10, 0),
    col_double     double,
    col_char       char(10),
    col_varchar    varchar(10),
    col_date       date,
    col_time       time,
    col_year       int,
    col_timestamp  timestamp,
    col_datetime   timestamp,
    col_tinyblob   bytes,
    col_blob       bytes,
    col_mediumblob bytes,
    col_longblob   bytes,
    col_tinytext   varchar,
    col_text       varchar,
    col_mediumtext varchar,
    col_longtext   varchar,
    col_enum       varchar,
    col_set        varchar,
    col_geometry   bytes,
    col_binary     bytes,
    col_varbinary  bytes,
    col_json       varchar
) WITH (
      'connector' = 'binlog-x'
      ,'username' = 'root'
      ,'password' = 'root'
      ,'cat' = 'insert,delete,update'
      ,'url' = 'jdbc:mysql://localhost:3306/tudou?useSSL=false'
      ,'host' = 'localhost'
      ,'port' = '3306'
--   ,'journal-name' = 'mysql-bin.000001'
      ,'table' = 'tudou.type'
      ,'timestamp-format.standard' = 'SQL'
      );

CREATE TABLE sink
(
    id             bigint,
    col_bit        BOOLEAN,
    col_tinyint    tinyint,
    col_smallint   int,
    col_mediumint  int,
    col_int        int,
    col_integer    int,
    col_bigint     bigint,
    col_real       double,
    col_float      float,
    col_decimal    decimal(10, 0),
    col_numric     decimal(10, 0),
    col_double     double,
    col_char       char(10),
    col_varchar    varchar(10),
    col_date       date,
    col_time       time,
    col_year       int,
    col_timestamp  timestamp,
    col_datetime   timestamp,
    col_tinyblob   bytes,
    col_blob       bytes,
    col_mediumblob bytes,
    col_longblob   bytes,
    col_tinytext   varchar,
    col_text       varchar,
    col_mediumtext varchar,
    col_longtext   varchar,
    col_enum       varchar,
    col_set        varchar,
    col_geometry   bytes,
    col_binary     bytes,
    col_varbinary  bytes,
    col_json       varchar
) WITH (
      'connector' = 'stream-x'
      );

insert into sink
select *
from source u;
