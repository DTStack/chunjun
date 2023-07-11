CREATE TABLE source
(
    id int,
    dt varchar,
    proc_time AS PROCTIME()
) WITH (
      'connector' = 'mysql-x',
      'url' = 'jdbc:mysql://mysql:3306/tiezhu?useSSL=false',
      'table-name' = 'test_six',
      'username' = 'root',
      'password' = '',
      'sink.buffer-flush.interval' = '1000',
      'sink.all-replace' = 'false',
      'sink.buffer-flush.max-rows' = '100',
      'sink.parallelism' = '1');

CREATE TABLE target
(
    id              BIGINT,
    alert_gate_name VARCHAR
) WITH (
      'connector' = 'stream-x',
      'print' = 'true'
      );

CREATE TABLE side
(
    name VARCHAR
) WITH (
      'schema' = 'beihai_prod',
      'password' = 'dev123',
      'connector' = 'doris-x',
      'table-name' = 'test_beihai_1',
      'sink.parallelism' = '1',
      'url' = 'jdbc:mysql://doris_fe:3306/dt_pub_service_dev42?useSSL=false',
      'username' = 'dev',
      'lookup.error-limit' = '100',
      'lookup.cache-type' = 'LRU',
      'lookup.parallelism' = '1',
      'vertx.worker-pool-size' = '5',
      'lookup.cache.ttl' = '60000',
      'lookup.cache.max-rows' = '10000'
      );
insert
into target
select id,
       source.dt as alert_gate_name
from source
         left join
     side
     on side.name = source.dt;
