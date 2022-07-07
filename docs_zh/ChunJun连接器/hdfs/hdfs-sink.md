# HDFS Sink

## 一、介绍
HDFS插件支持直接从配置的HDFS路径上读取及写入TextFile、Orc、Parquet类型的文件，一般配合HIve表使用。如：读取Hive表某分区下所有数据，实质是读取Hive表对应分区的HDFS路径下的数据文件；将数据写入Hive表某分区，实质是直接将数据文件写入到对应分区的HDFS路径下；HDFS插件不会对Hive表进行任何DDL操作。

HDFS Sink在开启checkpoint时会使用二阶段提交，预提交时将.data目录中生成的数据文件复制到正式目录中并标记复制的数据文件，提交阶段删除.data目录中标记的数据文件，回滚时删除正式目录中标记的数据文件。


## 二、支持版本
Hadoop 2.x、Hadoop 3.x


## 三、插件名称
| Sync | hdfssink、hdfswriter |
| --- | --- |
| SQL | hdfs-x |


## 四、参数说明

### 1、Sync
- **path**
  - 描述：写入的数据文件的路径
  - 注意：真正写入的文件路径是 path/fileName
  - 必选：是
  - 参数类型：string
  - 默认值：无
  <br />

- **fileName**
  - 描述：数据文件目录名称,可以传递分区(eg: pt=2022)
  - 注意：真正写入的文件路径是 path/fileName
  - 必选：否
  - 参数类型：string
  - 默认值：无
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

- **column**
  - 描述：需要读取的字段
  - 注意：不支持*格式
  - 格式：
    ```text
    "column": [{
        "name": "col",
        "type": "string",
        "index":1,
        "isPart":false,
        "format": "yyyy-MM-dd hh:mm:ss",
        "value": "value"
    }]
    ```

- 属性说明:
  - name：必选，字段名称
  - type：必选，字段类型，需要和数据文件中实际的字段类型匹配
  - index：非必选，字段在所有字段中的位置索引，从0开始计算，默认为-1，按照数组顺序依次读取，配置后读取指定字段列
  - isPart：非必选，是否是分区字段，如果是分区字段，会自动从path上截取分区赋值，默认为fale
  - format：非必选，按照指定格式，格式化日期
  - value：非必选，常量字段，将value的值作为常量列返回
- 必选：是
- 参数类型：数组
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

- **fullColumnName**
  - 描述：写入的字段名称
  - 必须：否
  - 字段类型：list
  - 默认值：column的name集合
<br />

- **fullColumnType**
  - 描述：写入的字段类型
  - 必须：否
  - 字段类型：list
  - 默认值：column的type集合
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
  - 默认值：`1073741824`（1G）
<br />

- **nextCheckRows**
  - 描述：下一次检查文件大小的间隔条数，每达到该条数时会查询当前写入文件的文件大小
  - 必选：否
  - 字段类型：long
  - 默认值：`5000`
<br />

- **rowGroupSIze**
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

    
### 2、SQL
- **path**
  - 描述：写入的数据文件的路径
  - 注意：真正写入的文件路径是 path/fileName
  - 必选：是
  - 参数类型：string
  - 默认值：无
    <br />

- **file-name**
  - 描述：数据文件目录名称
  - 注意：真正写入的文件路径是 path/fileName
  - 必选：否
  - 参数类型：string
  - 默认值：无
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

- **column**
  - 描述：需要读取的字段
  - 注意：不支持*格式
  - 格式：
```text
"column": [{
    "name": "col",
    "type": "string",
    "index":1,
    "isPart":false,
    "format": "yyyy-MM-dd hh:mm:ss",
    "value": "value"
}]
```

- 属性说明:
  - name：必选，字段名称
  - type：必选，字段类型，需要和数据文件中实际的字段类型匹配
  - index：非必选，字段在所有字段中的位置索引，从0开始计算，默认为-1，按照数组顺序依次读取，配置后读取指定字段列
  - isPart：非必选，是否是分区字段，如果是分区字段，会自动从path上截取分区赋值，默认为fale
  - format：非必选，按照指定格式，格式化日期
  - value：非必选，常量字段，将value的值作为常量列返回
- 必选：是
- 参数类型：数组
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

- **sink.parallelism**
  - 描述：sink的并行度
  - 必选：否
  - 参数类型：String
  - 默认值：无
    <br />


## 五、数据类型
| 支持 | BOOLEAN、TINYINT、SMALLINT、INT、BIGINT、FLOAT、DOUBLE、DECIMAL、STRING、VARCHAR、CHAR、TIMESTAMP、DATE、BINARY |
| --- | --- |
| 暂不支持 | ARRAY、MAP、STRUCT、UNION |


## 六、脚本示例
见项目内`chunjun-examples`文件夹。

