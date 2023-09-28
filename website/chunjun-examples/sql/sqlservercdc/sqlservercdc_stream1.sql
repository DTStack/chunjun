CREATE TABLE source
(
    id int,
    x1 boolean,
    x2 bigInt,
    x3 tinyInt,
    x4 double ,
    x5 decimal(10,2),
    x6 decimal(10,2),
    x7 varchar,
    x8 char,
    x9 varchar,
    x10 varchar,
    x11 date,
    x12 timestamp,
    x13 timestamp,
    x14 time,
    x15 timestamp,
    x17 varchar,
    x18 float
) WITH (
      'connector' = 'sqlservercdc-x'
      ,'username' = 'test'
      ,'password' = 'Abc12345'
      ,'cat' = 'insert,delete,update'
      ,'url' = 'jdbc:sqlserver://localhost:1433;databaseName=db_test'
      ,'table' = 'test.test123'
      ,'timestamp-format.standard' = 'SQL'
      ,'database' = 'db_test'
      ,'poll-interval' = '1000'
      );

CREATE TABLE sink
(
     id int,
    x1 boolean,
    x2 bigInt,
    x3 tinyInt,
    x4 double ,
    x5 decimal(10,2),
    x6 decimal(10,2),
    x7 varchar,
    x8 char,
    x9 varchar,
    x10 varchar,
    x11 date,
    x12 timestamp,
    x13 timestamp,
    x14 time,
    x15 timestamp,
    x17 varchar,
    x18 float
) WITH (
      'connector' = 'stream-x'
      );

insert into sink
select *
from source u;
