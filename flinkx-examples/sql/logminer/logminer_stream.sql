CREATE TABLE source
(
    ID             bigint,
    TTIME          DATE,
    TTIMESTAMP     TIMESTAMP
) WITH (
      'connector' = 'oraclelogminer-x'
      ,'url' = 'jdbc:oracle:thin:@127.0.0.1:1521:xe'
      ,'username' = 'username'
      ,'password' = 'password'
      ,'cat' = 'insert,delete,update'
      ,'table' = 'schema.table'
      ,'timestamp-format.standard' = 'SQL'
      );

CREATE TABLE sink
(
    ID             bigint,
    TTIME          DATE,
    TTIMESTAMP     TIMESTAMP
) WITH (
      'connector' = 'stream-x'
      );

insert into sink
select *
from source u;
