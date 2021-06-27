CREATE TABLE source_one
(
    id        int,
    name      varchar,
    age       int,
    birth     timestamp,
    todayTime time,
    todayDate date,
    money     decimal,
    price     double,
    wechat    varchar,
    proName   varchar,
    PROCTIME AS PROCTIME()
) WITH (
      'connector' = 'kafka-x'
      ,'topic' = 'tiezhu_test'
      ,'properties.bootstrap.servers' = 'kudu1:9092'
      ,'properties.group.id' = 'tiezhu_one'
      ,'scan.startup.mode' = 'earliest-offset'
--       ,'scan.startup.mode' = 'latest-offset'
      ,'format' = 'json'
      ,'json.timestamp-format.standard' = 'SQL'
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
      'host' = '172.16.100.238',
      'port' = '9042',
      'user-name' = 'cassandra',
      'password' = 'cassandra',
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
