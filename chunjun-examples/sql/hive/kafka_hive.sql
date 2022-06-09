-- {"id":1,"col_bit":true,"col_tinyint":127,"col_smallint":32767,"col_int":2147483647,"col_bigint":1,"col_float":1.1,"col_double":1.1,"col_decimal":1,"col_string":"string","col_varchar":"varchar","col_char":"char","col_timestamp":"2020-07-30 10:08:22","col_date":"2020-07-30"}

CREATE TABLE source
(
    id             bigint,
    col_bit        boolean,
    col_tinyint    tinyint,
    col_smallint   smallint,
    col_int        int,
    col_bigint     bigint,
    col_float      float,
    col_double     double,
    col_decimal    decimal(10, 0),
    col_string     char(10),
    col_varchar    varchar(10),
    col_char       char(10),
--     col_binary     binary,
    col_timestamp  timestamp,
    col_date       date
) WITH (
      'connector' = 'kafka-x'
      ,'topic' = 'tudou'
      ,'properties.bootstrap.servers' = 'ip:9092'
      ,'scan.startup.mode' = 'latest-offset'
      ,'format' = 'json'
      ,'json.timestamp-format.standard' = 'SQL'
);

CREATE TABLE sink
(
    id              bigint,
    col_boolean     boolean,
    col_tinyint     tinyint,
    col_smallint    smallint,
    col_int         int,
    col_bigint      bigint,
    col_float       float,
    col_double      double,
    col_decimal     decimal(10, 0),
    col_string      char(10),
    col_varchar     varchar(10),
    col_char        char(10),
--     col_binary      binary,
    col_timestamp   timestamp,
    col_date        date
) WITH (
      'connector' = 'hive-x'
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

      ,'url' = 'jdbc:hive2://ip:10000/tudou'
      ,'username' = ''
      ,'password' = ''
      ,'partition' = 'pt'
      ,'partition-type' = 'DAY'
      ,'table-name' = 'kudu'
      );

insert into sink
select *
from source u;
