CREATE TABLE source
(
    id int,
    name varchar(22),
    day1 date,
    day2 timestamp,
    sale decimal(10,3)

) WITH (
      'connector' = 'sqlservercdc-x'
      ,'username' = 'test'
      ,'password' = 'Abc12345'
      ,'cat' = 'insert,delete,update'
      ,'url' = 'jdbc:sqlserver://localhost:1433;databaseName=db_test'
      ,'table' = 'test.shifang2'
      ,'timestamp-format.standard' = 'SQL'
      ,'database' = 'db_test'
      ,'poll-interval' = '1000'
      );

CREATE TABLE sink
(
     id int,
    name varchar(22),
    day1 date,
    day2 timestamp,
    sale decimal(10,3)
) WITH (
      'connector' = 'stream-x'
      );

insert into sink
select *
from source u;
