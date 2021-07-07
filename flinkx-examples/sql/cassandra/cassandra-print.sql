CREATE TABLE source_one
(
    id        int,
    name      varchar,
    birth     timestamp,
    todayTime time,
    todayDate date
) WITH (
      'connector' = 'cassandra-x',
      'host' = 'ip1,ip2,ip3',
      'port' = '9042',
      'hostDistance' = 'local',
      'user-name' = 'cassandra',
      'password' = 'xxxxxxxx',
      'table-name' = 'one',
      'keyspaces' = 'tiezhu',
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
