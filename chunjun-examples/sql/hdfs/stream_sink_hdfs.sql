CREATE TABLE source
(
    id int,
    col_boolean boolean,
    col_tinyint tinyint
) WITH (
      'connector' = 'stream-x'
      ,'number-of-rows' = '10000'
);

CREATE TABLE sink
(
    id int,
    col_boolean boolean
) WITH (
      'connector' = 'hdfs-x'
      ,'path' = 'hdfs://ns/user/hive/warehouse/tudou.db/type_txt'
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
      ,'file-type' = 'text'
      -- 为了处理多任务（HDFS、HIVE）同时输出到同一数据源时数据丢失的问题
      -- 每个任务指定唯一的字符标识，且不能变化
      ,'properties.job-identifier' = 'job_id_tmp'
);

insert into sink
select *
from source u;
