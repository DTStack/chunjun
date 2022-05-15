CREATE TABLE source_one
(
    id        int,
    name      varchar,
    birth     timestamp,
    todayTime time,
    todayDate date,
    PROCTIME AS PROCTIME()
) WITH (
      'connector' = 'cassandra-x',
      'host' = 'ip1,ip2,ip3',
      'port' = '9042',
      'hostDistance' = 'local',
      'user-name' = 'cassandra',
      'password' = 'xxxxxxxx',
      'table-name' = 'one',
      'keyspaces' = 'tiezhu'
      );


CREATE TABLE side_one
(
    id        int,
    name      varchar,
    birth     timestamp,
    todayTime time,
    todayDate date,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
      'connector' = 'cassandra-x',
      'host' = 'ip1,ip2,ip3',
      'port' = '9042',
      'hostDistance' = 'local',
      'user-name' = 'cassandra',
      'password' = 'xxxxxxxx',
      'table-name' = 'one',
      'keyspaces' = 'tiezhu',
      'lookup.cache-type' = 'all'
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

CREATE VIEW view_out AS
SELECT u.id        AS id,
       u.name      AS name,
       s.birth     AS birth,
       u.todayTime AS todayTime,
       s.todayDate AS todayDate
FROM source_one s
         JOIN side_one FOR SYSTEM_TIME AS OF s.PROCTIME AS u
              ON s.id = u.id;


INSERT INTO sink_one
SELECT *
FROM view_out;
