CREATE TABLE source
(
    id          bigint,
    user_id     bigint,
    name        STRING
) WITH (
      'connector' = 'mysql-x',
      'url' = 'jdbc:mysql://ip:3308/tudou?useSSL=false',
      'table-name' = 'kudu1',
      'username' = 'username',
      'password' = 'password'
      ,'scan.fetch-size' = '2'
      ,'scan.query-timeout' = '10'

      ,'scan.polling-interval' = '3000' --间隔轮训时间。非必填(不填为离线任务)，无默认

      ,'scan.parallelism' = '3' -- 并行度
--       ,'scan.fetch-size' = '2' -- 每次从数据库中fetch大小。默认：1024条
--       ,'scan.query-timeout' = '10' -- 数据库连接超时时间。默认：不超时

      ,'scan.partition.column' = 'id' -- 多并行度读取的切分字段，多并行度下必需要设置。无默认
      ,'scan.partition.strategy' = 'mod' -- 数据分片策略。默认：range，如果并行度大于1，且是增量任务或者间隔轮询，则会使用mod分片

      ,'scan.increment.column' = 'id' -- 增量字段名称，必须是表中的字段。非必填，无默认
--       ,'scan.increment.column-type' = 'int'  -- 增量字段类型。非必填，无默认
      ,'scan.start-location' = '100' -- 增量字段开始位置,如果不指定则先同步所有，然后在增量。非必填，无默认，如果没配置scan.increment.column，则不生效

      ,'scan.restore.columnname' = 'id' -- 开启了cp，任务从sp/cp续跑字段名称。如果续跑，则会覆盖scan.start-location开始位置，从续跑点开始。非必填，无默认
--       ,'scan.restore.columntype' = 'int' -- 开启了cp，任务从sp/cp续跑字段类型。非必填，无默认
      );

CREATE TABLE sink
(
    id bigint,
    user_id bigint,
    name string
) WITH (
      'connector' = 'hdfs-x'
      ,'path' = 'hdfs://ns/user/hive/warehouse/tudou.db/kudu_txt'
      ,'fileName' = 'pt=1'
      ,'properties.hadoop.user.name' = 'root'
      ,'properties.dfs.ha.namenodes.ns' = 'nn1,nn2'
      ,'properties.fs.defaultFS' = 'hdfs://ns'
      ,'properties.dfs.namenode.rpc-address.ns.nn2' = 'ip:9000'
      ,'properties.dfs.client.failover.proxy.provider.ns' = 'org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider'
      ,'properties.dfs.namenode.rpc-address.ns.nn1' = 'ip:9000'
      ,'properties.dfs.nameservices' = 'ns'
      ,'properties.fs.hdfs.impl.disable.cache' = 'true'
      ,'properties.fs.hdfs.impl' = 'org.apache.hadoop.hdfs.DistributedFileSystem'
      ,'defaultFS' = 'hdfs://ns'
      ,'fieldDelimiter' = ','
      ,'encoding' = 'utf-8'
      ,'maxFileSize' = '10485760'
      ,'nextCheckRows' = '20000'
      ,'writeMode' = 'overwrite'
      ,'fileType' = 'text'

      ,'sink.parallelism' = '3'
      );

insert into sink
select *
from source u;
