CREATE TABLE mt
(
    id   int,
    name varchar,
    proc_time AS PROCTIME()
) WITH (
      'properties.bootstrap.servers' = 'kafka01:9092',
      'connector' = 'kafka-x',
      'scan.parallelism' = '1',
      'format' = 'json',
      'scan.startup.timestamp-millis' = '1650871857000',
      'topic' = 'chen_par',
      'scan.startup.mode' = 'timestamp'
      );
CREATE TABLE mywb
(
    id   INT,
    name VARCHAR,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
      'schema' = 'cx',
      'connector' = 'doris-x',
      'lookup.cache-type' = 'LRU',
      'lookup.parallelism' = '1',
      'vertx.worker-pool-size' = '5',
      'lookup.cache.ttl' = '60000',
      'lookup.cache.max-rows' = '10000',
      'table-name' = 'dw1',
      'url' = 'jdbc:mysql://doris_fe:9030/cx',
      'username' = 'root'
      );
CREATE TABLE MyResult
(
    id   INT,
    name VARCHAR
) WITH (
      'connector' = 'stream-x'
      );
INSERT
into MyResult
select m.id,
       w1x.name
from mt m
         left join
     mywb FOR SYSTEM_TIME AS OF m.proc_time AS w1x
     on w1x.id = m.id;
