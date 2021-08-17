CREATE TABLE source
(
    id int,
    col_boolean boolean,
    col_tinyint tinyint,
    col_smallint smallint,
    col_int int,
    col_bigint bigint,
    col_float float,
    col_double double,
    col_decimal decimal,
    col_string string,
    col_varchar varchar(255),
    col_char char(255),
--     col_binary binary,
    col_timestamp timestamp,
    col_date date
) WITH (
      'connector' = 'stream-x'
    ,'number-of-rows' = '1000'
);

CREATE TABLE sink
(
    id int,
    col_boolean boolean,
    col_tinyint tinyint,
    col_smallint smallint,
    col_int int,
    col_bigint bigint,
    col_float float,
    col_double double,
    col_decimal decimal,
    col_string string,
    col_varchar varchar(255),
    col_char char(255),
--     col_binary binary,
    col_timestamp timestamp,
    col_date date
) WITH (
      'connector' = 'hdfs-x'
      ,'path' = 'hdfs://ns/user/hive/warehouse/tudou.db/type_parquet'
      ,'file-name' = 'pt=1'
      ,'properties.hadoop.user.name' = 'root'
      ,'properties.dfs.ha.namenodes.ns' = 'nn1,nn2'
      ,'properties.fs.defaultFS' = 'hdfs://ns'
      ,'properties.dfs.namenode.rpc-address.ns.nn2' = 'ip:9000'
      ,'properties.dfs.client.failover.proxy.provider.ns' = 'org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider'
      ,'properties.dfs.namenode.rpc-address.ns.nn1' = 'ip:9000'
      ,'properties.dfs.nameservices' = 'ns'
      ,'properties.fs.hdfs.impl.disable.cache' = 'true'
      ,'properties.fs.hdfs.impl' = 'org.apache.hadoop.hdfs.DistributedFileSystem'
      ,'default-fs' = 'hdfs://ns'
      ,'field-delimiter' = ','
      ,'encoding' = 'utf-8'
      ,'max-file-size' = '10485760'
      ,'next-check-rows' = '20000'
      ,'write-mode' = 'overwrite'
      ,'file-type' = 'parquet'
);

insert into sink
select *
from source u;
