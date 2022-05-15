CREATE TABLE k_source
(
    id        varchar,
    name      STRING,
    age       bigint,
    PROCTIME AS PROCTIME()
) WITH (
      'connector' = 'kafka-x'
      ,'topic' = 'da'
      ,'properties.bootstrap.servers' = 'localhost:9092'
      ,'properties.group.id' = 'luna_g'
      ,'scan.startup.mode' = 'earliest-offset'
      -- ,'scan.startup.mode' = 'latest-offset'
      ,'format' = 'json'
      ,'json.timestamp-format.standard' = 'SQL'
      );


CREATE TABLE es_lookup
(
    id varchar,
    birthday TIMESTAMP
) WITH (
    'connector' ='es-x'
   ,'hosts' ='localhost:9200',
    'index' ='testdate9',
    'document-type' = '_doc',
    'lookup.cache-type' = 'all'
);


CREATE TABLE sink
( id varchar,
  name varchar,
   age bigint,
   esid varchar,
   birthday timestamp)
WITH(
'connector' = 'stream-x'
);

INSERT INTO
sink
SELECT a.id,
       a.name,
       a.age,
       b.id as esid,
       b.birthday
FROM k_source a
LEFT JOIN es_lookup FOR SYSTEM_TIME AS OF a.PROCTIME AS b ON a.id = b.id;
