CREATE TABLE source
(
    appkey  varchar,
    day_id  int,
    channel varchar,
    pv      int,
    uv      int,
    retry_done    int
) WITH (
      'connector' = 'mysql-x'
      ,'url' = 'jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=utf8&useSSL=false'
      ,'table-name' = 'starrocks_test'
      ,'username' = 'root'
      ,'password' = 'root'
      );

CREATE TABLE sink
(
    name    varchar,
    type    int,
    attr    varchar,
    score   int,
    quarter int,
    num     int
) WITH (
      'connector' = 'starrocks-x'
      ,'jdbc-url'='jdbc:mysql://localhost:9030/test?useUnicode=true&characterEncoding=utf8&useSSL=false&rewriteBatchedStatements=true&&serverTimezone=Asia/Shanghai'
      ,'load-url'='localhost'
      ,'database-name' = 'test'
      ,'table-name' = 'starrocks_sink_test'
      ,'username' = 'root'
      ,'password' = 'root'
      );

insert into sink
select appkey,day_id,channel,pv,uv, 1 as retry_done
from source;
