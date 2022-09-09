CREATE TABLE table1
(
    id            int,
    name          string,
    date_data     date,
    datetime_data timestamp(0),
    proctime AS PROCTIME()
) with (
      'connector' = 'mysql-x',
      'url' = 'jdbc:mysql://localhost:3306/test?useSSL=false&allowPublicKeyRetrieval=true',
      'schema-name' = 'test',
      'table-name' = 'source',
      'username' = 'root',
      'password' = 'root'
      );

CREATE TABLE side
(
    id            int,
    date_data     date,
    datetime_data timestamp(0)
) with (
      'connector' = 'starrocks-x',
      'url' = 'jdbc:mysql://node1:9030',
      'fe-nodes' = 'node1:8030;node2:8030;node3:8030',
      'schema-name' = 'test',
      'table-name' = 'side',
      'lookup.cache-type' = 'ALL',
      'username' = 'root',
      'password' = ''
      );

CREATE TABLE sink
(
    id        int PRIMARY KEY,
    date_data1 date ,
    date_data2 date
) WITH (
      'connector' = 'stream-x'
      );

insert into sink
select u.id
     , u.date_data
     , s.date_data
from table1 u
         left join side FOR SYSTEM_TIME AS OF u.proctime AS s
                   on u.id = s.id and u.datetime_data=s.datetime_data and u.date_data = s.date_data;

