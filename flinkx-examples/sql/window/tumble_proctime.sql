-- {"id":100,"name":"lb james阿道夫","money":293.899778,"datethree":"2020-07-30 10:08:22.123"}

CREATE TABLE source
(
    id        int,
    name      varchar,
    money     decimal,
    datethree TIMESTAMP(3),
    proctime as PROCTIME(),
    WATERMARK FOR datethree AS datethree - INTERVAL '3' SECOND
) WITH (
      'connector' = 'kafka-x'
      ,'topic' = 'chuixue'
      ,'properties.bootstrap.servers' = 'localhost:9092'
      ,'format' = 'json'
      ,'json.timestamp-format.standard' = 'SQL'
      );

CREATE TABLE sink
(
    id         int,
    name       varchar,
    money      decimal,
    start_time varchar,
    end_time   varchar,
    ctime_proc varchar
) WITH (
      'connector' = 'stream-x'
      );

insert into sink
SELECT u.id,
       max(name)                                                                         as name,
       sum(u.money)                                                                      as money,
       DATE_FORMAT(TUMBLE_START(u.proctime, INTERVAL '3' SECOND), 'yyyy-MM-dd HH:mm:ss') as start_time,
       DATE_FORMAT(TUMBLE_END(u.proctime, INTERVAL '3' SECOND), 'yyyy-MM-dd HH:mm:ss')   as end_time,
       last_value(cast(u.proctime as VARCHAR))                                           as ctime_proc
FROM source u
group by TUMBLE(u.proctime, INTERVAL '3' SECOND), u.id;



