CREATE TABLE source
(
    id      STRING,
    dateone   timestamp,
    age       bigint,
    score float,
    dtdate    date,
    dttime    time,
    flag BOOLEAN
) WITH (
      'connector' = 'stream-x',
      'number-of-rows' = '10', 
      'rows-per-second' = '1' 
      );

CREATE TABLE sink (
  vid   varchar, -- 字段中必须包含
  dateone   timestamp,
    age       bigint,
    score float,
    dtdate    date,
    dttime    time,
    flag BOOLEAN
) WITH (
  'connector' = 'nebula-x',
  'nebula.enableSSL' = 'false',
  'nebula.storage-addresses' = 'localhost:9559',
  'nebula.graphd-addresses' = 'localhost:9669',
  'nebula.password' = 'nebula',
  'nebula.username' = 'nebula',
  'nebula.schema-name' = 'test_vertex',
  'nebula.space' = 'chunjun_test',
  'nebula.schema-type' = 'vertex',
  'write.tasks' = '1',
  'nebula.bulk-size' = '5',
  'nebula.vid-type' = 'FIXED_STRING(36)',
  'write-mode' = 'upsert'
);

insert into sink
select * from source;
