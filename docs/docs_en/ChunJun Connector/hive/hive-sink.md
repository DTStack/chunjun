# Hive Sink

## 一、介绍
ChunJun只有Hive sink插件，没有Hive source插件，如需要读取Hive表中的数据，请使用HDFS source插件。

Hive sink插件支持实时地往Hive表中写数据，支持自动建表并根据当前系统时间自动创建分区，支持动态解析表名及分组映射，根据映射规则将不同的数据写入不同的Hive表。

Hive sink插件一般配合实时采集(CDC)插件、kafka source插件等实时类的插件一起使用。

Hive sink插件底层依赖HDFS sink，其基本原理也是向指定的HDFS路径中写入数据文件，可以看做是在HDFS sink上做了一些自动建表建分区及分组映射等拓展功能。

Hive sink插件使用时需要开启checkpoint，在checkpoint后数据才能在Hive表中被查出。在开启checkpoint时会使用二阶段提交，预提交时将.data目录中生成的数据文件复制到正式目录中并标记复制的数据文件，提交阶段删除.data目录中标记的数据文件，回滚时删除正式目录中标记的数据文件。


## 二、支持版本
Hive 1.x、Hive 2.x


## 四、参数说明

### 1、Sync
- **jdbcUrl**
  - 描述：连接Hive JDBC的字符串
  - 必选：是
  - 字段类型：string
  - 默认值：无
<br />

- **username**
  - 描述：Hive认证用户名
  - 必选：否
  - 字段类型：string
  - 默认值：无
<br />

- **password**
  - 描述：Hive认证密码
  - 必选：否
  - 字段类型：string
  - 默认值：无
<br />

- **partition**
  - 描述：分区字段名称
  - 必选：否
  - 字段类型：string
  - 默认值：`pt`
<br />

- **partitionType**
  - 描述：分区类型，包括 DAY、HOUR、MINUTE三种。**若分区不存在则会自动创建，自动创建的分区时间以当前任务运行的服务器时间为准**
    - DAY：天分区，分区示例：pt=20200101
    - HOUR：小时分区，分区示例：pt=2020010110
    - MINUTE：分钟分区，分区示例：pt=202001011027
  - 必选：否
  - 字段类型：string
  - 默认值：`DAY`
<br />
    
- **tablesColumn**
  - 描述：写入hive表的表结构信息，**若表不存在则会自动建表**。
  - 示例：
  - 必选：是
  - 字段类型：string
  - 默认值：无
```json
{
    "kudu":[
        {
            "key":"id",
            "type":"int"
        },
        {
            "key":"user_id",
            "type":"int"
        },
        {
            "key":"name",
            "type":"string"
        }
    ]
}
```
<br />

- **analyticalRules**
  - 描述： 建表的动态规则获取表名，按照${XXXX}的占位符，从待写入数据(map结构)里根据key XXX 获取值进行替换，创建对应的表，并将数据写入对应的表
  - 示例：stream_${schema}_${table}
  - 必选：否
  - 字段类型：string
  - 默认值：无
<br />

- **schema**
  - 描述： 自动建表时，analyticalRules里如果指定schema占位符，schema将此schema参数值进行替换
  - 必选：否
  - 字段类型：string
  - 默认值：无
<br />

- **distributeTable**
  - 描述：如果数据来源于各个CDC数据，则将不同的表进行聚合，多张表的数据写入同一个hive表
  - 必选：否
  - 字段类型：string
  - 默认值：无
  - 示例：
```json
 "distributeTable" : "{\"fenzu1\":[\"table1\"],\"fenzu2\":[\"table2\",\"table3\"]}",
```
table1的数据将写入hive表fenzu1里，table2和table3的数据将写入fenzu2里,如果配置distributeTable，则tablesColumn需要配置为如下格式：
```json
{
    "fenzu1":[
        {
            "key":"id",
            "type":"int"
        },
        {
            "key":"user_id",
            "type":"int"
        },
        {
            "key":"name",
            "type":"string"
        }
    ],
   "fenzu2":[
        {
            "key":"id",
            "type":"int"
        },
        {
            "key":"user_id",
            "type":"int"
        },
        {
            "key":"name",
            "type":"string"
        }
    ]
}
```
<br />

- **writeMode**
  - 描述：HDFS Sink写入前数据清理处理模式：
    - append：追加
    - overwrite：覆盖
  - 注意：overwrite模式时会删除hdfs当前目录下的所有文件
  - 必选：否
  - 字段类型：string
  - 默认值：append
<br />

- **fileType**
  - 描述：文件的类型，目前只支持用户配置为`text`、`orc`、`parquet`
    - text：textfile文件格式
    - orc：orcfile文件格式
    - parquet：parquet文件格式
  - 必选：是
  - 参数类型：string
  - 默认值：无
<br />


- **defaultFS**
  - 描述：Hadoop hdfs文件系统namenode节点地址。格式：hdfs://ip:端口；例如：hdfs://127.0.0.1:9000
  - 必选：是
  - 参数类型：string
  - 默认值：无
<br />

- **hadoopConfig**
  - 描述：集群HA模式时需要填写的core-site.xml及hdfs-site.xml中的配置，开启kerberos时包含kerberos相关配置
  - 必选：否
  - 参数类型：Map<String, Object>
  - 默认值：无
<br />

- **fieldDelimiter**
  - 描述：`fileType`为`text`时字段的分隔符
  - 必选：否
  - 参数类型：string
  - 默认值：`\001`
<br />

- **compress**
  - 描述：hdfs文件压缩类型
    - text：支持`GZIP`、`BZIP2`格式
    - orc：支持`SNAPPY`、`GZIP`、`BZIP`、`LZ4`格式
    - parquet：支持`SNAPPY`、`GZIP`、`LZO`格式
  - 注意：`SNAPPY`格式需要用户安装**SnappyCodec**
  - 必选：否
  - 字段类型：string
  - 默认值：
    - text 默认不进行压缩
    - orc 默认为ZLIB格式
    - parquet 默认为SNAPPY格式
<br />

- **maxFileSize**
  - 描述：写入hdfs单个文件最大大小，单位字节
  - 必选：否
  - 字段类型：long
  - 默认值：`1073741824‬`（1G）
<br />

- **nextCheckRows**
  - 描述：下一次检查文件大小的间隔条数，每达到该条数时会查询当前写入文件的文件大小
  - 必选：否
  - 字段类型：long
  - 默认值：`5000`
<br />

- **rowGroupSize**
  - 描述：`fileType`为`parquet`时定row group的大小，单位字节
  - 必须：否
  - 字段类型：int
  - 默认值：`134217728`（128M）
<br />

- **enableDictionary**
  - 描述：`fileType`为`parquet`时，是否启动字典编码
  - 必须：否
  - 字段类型：boolean
  - 默认值：`true`
<br />

- **encoding**
  - 描述：`fileType`为`text`时字段的字符编码
  - 必选：否
  - 字段类型：string
  - 默认值：`UTF-8`
<br />

### 2、SQL
- **url**
  - 描述：连接Hive JDBC的字符串
  - 必选：是
  - 字段类型：string
  - 默认值：无
    <br />

- **username**
  - 描述：Hive认证用户名
  - 必选：否
  - 字段类型：string
  - 默认值：无
    <br />

- **password**
  - 描述：Hive认证密码
  - 必选：否
  - 字段类型：string
  - 默认值：无
    <br />

- **partition**
  - 描述：分区字段名称
  - 必选：否
  - 字段类型：string
  - 默认值：`pt`
    <br />

- **partition-type**
  - 描述：分区类型，包括 DAY、HOUR、MINUTE三种。**若分区不存在则会自动创建，自动创建的分区时间以当前任务运行的服务器时间为准**
    - DAY：天分区，分区示例：pt=20200101
    - HOUR：小时分区，分区示例：pt=2020010110
    - MINUTE：分钟分区，分区示例：pt=202001011027
  - 必选：否
  - 字段类型：string
  - 默认值：`DAY`
    <br />

- **write-mode**
  - 描述：HDFS Sink写入前数据清理处理模式：
    - append：追加
    - overwrite：覆盖
  - 注意：overwrite模式时会删除hdfs当前目录下的所有文件
  - 必选：否
  - 字段类型：string
  - 默认值：append
    <br />

- **file-type**
  - 描述：文件的类型，目前只支持用户配置为`text`、`orc`、`parquet`
    - text：textfile文件格式
    - orc：orcfile文件格式
    - parquet：parquet文件格式
  - 必选：是
  - 参数类型：string
  - 默认值：无
    <br />

- **default-fs**
  - 描述：Hadoop hdfs文件系统namenode节点地址。格式：hdfs://ip:端口；例如：hdfs://127.0.0.1:9000
  - 必选：是
  - 参数类型：string
  - 默认值：无
    <br />

- **hadoopConfig**
  - 描述：集群HA模式时需要填写的core-site.xml及hdfs-site.xml中的配置，开启kerberos时包含kerberos相关配置
  - 必选：否
  - 配置方式：'properties.key' = 'value'，key为hadoopConfig中的key，value为hadoopConfig中的value，如下所示：
```
'properties.hadoop.user.name' = 'root',
'properties.dfs.ha.namenodes.ns' = 'nn1,nn2',
'properties.fs.defaultFS' = 'hdfs://ns',
'properties.dfs.namenode.rpc-address.ns.nn2' = 'ip:9000',
'properties.dfs.client.failover.proxy.provider.ns' = 'org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider',
'properties.dfs.namenode.rpc-address.ns.nn1' = 'ip:9000',
'properties.dfs.nameservices' = 'ns',
'properties.fs.hdfs.impl.disable.cache' = 'true',
'properties.fs.hdfs.impl' = 'org.apache.hadoop.hdfs.DistributedFileSystem'
```

- **field-delimiter**
  - 描述：`fileType`为`text`时字段的分隔符
  - 必选：否
  - 参数类型：string
  - 默认值：`\001`
    <br />

- **compress**
  - 描述：hdfs文件压缩类型
    - text：支持`GZIP`、`BZIP2`格式
    - orc：支持`SNAPPY`、`GZIP`、`BZIP`、`LZ4`格式
    - parquet：支持`SNAPPY`、`GZIP`、`LZO`格式
  - 注意：`SNAPPY`格式需要用户安装**SnappyCodec**
  - 必选：否
  - 字段类型：string
  - 默认值：
    - text 默认不进行压缩
    - orc 默认为ZLIB格式
    - parquet 默认为SNAPPY格式
      <br />

- **max-file-size**
  - 描述：写入hdfs单个文件最大大小，单位字节
  - 必选：否
  - 字段类型：long
  - 默认值：`1073741824`（1G）
    <br />

- **next-check-rows**
  - 描述：下一次检查文件大小的间隔条数，每达到该条数时会查询当前写入文件的文件大小
  - 必选：否
  - 字段类型：long
  - 默认值：`5000`
    <br />

- **enable-dictionary**
  - 描述：`fileType`为`parquet`时，是否启动字典编码
  - 必须：否
  - 字段类型：boolean
  - 默认值：`true`
    <br />

- **encoding**
  - 描述：`fileType`为`text`时字段的字符编码
  - 必选：否
  - 字段类型：string
  - 默认值：`UTF-8`
    <br />

- **table-name**
  - 描述：Hive表名
  - 必选：是
  - 字段类型：string
  - 默认值：无
<br />

## 五、数据类型
| 支持 | BOOLEAN、TINYINT、SMALLINT、INT、BIGINT、FLOAT、DOUBLE、DECIMAL、STRING、VARCHAR、CHAR、TIMESTAMP、DATE、BINARY |
| --- | --- |
| 暂不支持 | ARRAY、MAP、STRUCT、UNION |


## 六、脚本示例
见项目内`chunjun-examples`文件夹。
