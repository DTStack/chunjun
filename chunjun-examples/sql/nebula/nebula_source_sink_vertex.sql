CREATE TABLE source (
  vid   varchar,  -- 字段中必须包含
  ownerPhone varchar,
  ownerName string,
  ownerCerti varchar,
  image varchar,
  merchantId varchar,
  registeredVehicleId varchar,
  licencePlate varchar
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
  'lookup.cache-type' = 'lru',
  'read.tasks' = '4',
  'nebula.schema-type' = 'vertex'
);

CREATE TABLE sink (
  vid   varchar,  -- 字段中必须包含
  ownerPhone varchar,
  ownerName string,
  ownerCerti varchar,
  image varchar,
  merchantId varchar,
  registeredVehicleId varchar,
  licencePlate varchar,
 primary key (vid) not enforced
) WITH (
  'connector' = 'nebula-x',
  'nebula.enableSSL' = 'false',
  'nebula.storage-addresses' = 'localhost:9559',
  'nebula.graphd-addresses' = 'localhost:9669',
  'nebula.password' = 'nebula',
  'nebula.username' = 'nebula',
  'nebula.schema-name' = 'other',
  'nebula.fatch-size' = '100',
  'nebula.space' = 'test',
  'nebula.schema-type' = 'vertex',
  'write.tasks' = '4',
  'nebula.bulk-size' = '5',
  'nebula.vid-type' = 'FIXED_STRING(36)',
  'write-mode' = 'upsert'
);


insert into sink 
select
  *
from source limit 100;
