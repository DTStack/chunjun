
CREATE TABLE MyResult
(
    id   INT,
    name INT
) WITH (
      'print' = 'true',
      'connector' = 'stream-x'
      );
CREATE TABLE mywb
(
    id   INT,
    name INT,
    age  INT,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
      'password' = 'admin123',
      'connector' = 'mysql-x',
      'lookup.cache-type' = 'LRU',
      'lookup.parallelism' = '1',
      'vertx.worker-pool-size' = '5',
      'lookup.cache.ttl' = '60000',
      'lookup.cache.max-rows' = '10000',
      'table-name' = 'test_five',
      'url' = 'jdbc:mysql://k3:3306/tiezhu?useSSL=false',
      'username' = 'root'
      );
insert
into MyResult
select yb.id,
       wb.name
from MyTable yb
         left join
     mywb FOR SYSTEM_TIME AS OF yb.proc_time AS wb
     on yb.id = wb.id;
