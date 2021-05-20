CREATE TABLE source
(
    ID             bigint,
    TTIME          DATE,
    TTIMESTAMP     TIMESTAMP
) WITH (
      'connector' = 'oraclelogminer-x'
      ,'jdbcUrl' = 'jdbc:oracle:thin:@172.16.100.220:1521:xe'
      ,'username' = 'tudou'
      ,'password' = 'abc123'
      ,'cat' = 'insert,delete,update'
      ,'listenerTables' = 'TUDOU.TIMETEST'
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
