CREATE TABLE source (
  srcId   varchar,   -- 字段中必须包含
  dstId varchar,     -- 字段中必须包含
  `rank` bigint,     -- 字段中必须包含
  personId varchar
) WITH (
  'connector' = 'nebula-x',
  'nebula.enableSSL' = 'false',
  'nebula.storage-addresses' = 'localhost:9559',
  'nebula.graphd-addresses' = 'localhost:9669',
  'nebula.password' = 'nebula',
  'nebula.username' = 'nebula',
  'nebula.schema-name' = 'test',
  'nebula.fatch-size' = '100',
  'nebula.space' = 'test',
  'nebula.schema-type' = 'edge'
);

CREATE TABLE sink (
  srcId   varchar, -- 字段中必须包含
  dstId varchar,   -- 字段中必须包含
  `rank` bigint,   -- 可有可无
  personId varchar
) WITH (
  'connector' = 'nebula-x',
  'nebula.enableSSL' = 'false',
  'nebula.storage-addresses' = 'localhost:9559',
  'nebula.graphd-addresses' = 'localhost:9669',
  'nebula.password' = 'nebula',
  'nebula.username' = 'nebula',
  'nebula.schema-name' = 'test',
  'nebula.space' = 'other',
  'nebula.schema-type' = 'edge',
  'write.tasks' = '1',
  'nebula.bulk-size' = '5',
  'write-mode' = 'upsert'
);


insert into sink select * from source;
