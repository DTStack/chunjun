CREATE TABLE source_one
(
    id        int,
    name      varchar,
    birth     timestamp,
    todayTime time,
    todayDate date
) WITH (
      'connector' = 'cassandra-x',
      'host' = '172.16.100.238,172.16.100.244,172.16.100.67',
      'port' = '9042',
      'user-name' = 'cassandra',
      'password' = 'cassandra',
      'table-name' = 'one',
      'keyspaces' = 'tiezhu'
      );

CREATE TABLE sink_one
(
    id        int,
    name      varchar,
    birth     timestamp,
    todayTime time,
    todayDate date
) WITH (
      'connector' = 'stream-x'
      );


INSERT INTO sink_one
SELECT id,
       name,
       birth,
       todayTime,
       todayDate
FROM source_one;
