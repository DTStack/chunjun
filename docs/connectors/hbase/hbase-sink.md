# HBase Sink

## 一、介绍
HBase sink

## 二、支持版本
HBase 1.4 +


## 三、插件名称
| Sync | hbasesink、hbasewriter |
| --- | --- |
| SQL | hbase1.4-x |


## 四、参数说明
### 1、Sync
- **tablename**
    - 描述：hbase表名
    - 必选：是
    - 默认值：无



- **hbaseConfig**
    - 描述：hbase的连接配置，以json的形式组织 (见hbase-site.xml)，key可以为以下七种：

Kerberos；<br />hbase.security.authentication；<br />hbase.security.authorization；<br />hbase.master.kerberos.principal；<br />hbase.master.keytab.file；<br />hbase.regionserver.keytab.file；<br />hbase.regionserver.kerberos.principal

- 必选：是
- 默认值：无



- **nullMode**
    - 描述：读取的null值时，如何处理。支持两种方式：
        - skip：表示不向hbase写这列；
        - empty：写入HConstants.EMPTY_BYTE_ARRAY，即new byte [0]
    - 必选：否
    - 默认值：skip



- **encoding**
    - 描述：字符编码
    - 必选：无
    - 默认值：UTF-8

<br />

- **walFlag**
    - 描述：在HBae client向集群中的RegionServer提交数据时（Put/Delete操作），首先会先写WAL（Write Ahead Log）日志（即HLog，一个RegionServer上的所有Region共享一个HLog），只有当WAL日志写成功后，再接着写MemStore，然后客户端被通知提交数据成功；如果写WAL日志失败，客户端则被通知提交失败。关闭（false）放弃写WAL日志，从而提高数据写入的性能
    - 必选：否
    - 默认值：false



- **writeBufferSize**
    - 描述：设置HBae client的写buffer大小，单位字节。配合autoflush使用。autoflush，开启（true）表示Hbase client在写的时候有一条put就执行一次更新；关闭（false），表示Hbase client在写的时候只有当put填满客户端写缓存时，才实际向HBase服务端发起写请求
    - 必选：否
    - 默认值：8388608（8M）



- **scanCacheSize**
    - 描述：一次RPC请求批量读取的Results数量
    - 必选：无
    - 默认值：256



- **scanBatchSize**
    - 描述：每一个result中的列的数量
    - 必选：无
    - 默认值：100



- **column**
    - 描述：要读取的hbase字段，normal 模式与multiVersionFixedColumn 模式下必填项。
        - name：指定读取的hbase列，除了rowkey外，必须为 列族:列名 的格式；
        - type：指定源数据的类型，format指定日期类型的格式，value指定当前类型为常量，不从hbase读取数据，而是根据value值自动生成对应的列。
    - 必选：是
    - 默认值：无



- **rowkeyColumn**
    - 描述：用于构造rowkey的描述信息，支持两种格式，每列形式如下
        - 字符串格式
          <br />字符串格式为：$(cf:col)，可以多个字段组合：$(cf:col1)_$(cf:col2)，
          <br />可以使用md5函数：md5($(cf:col))
        - 数组格式
            - 普通列
```
{
  "index": 0,  // 该列在column属性中的序号，从0开始
  "type": "string" 列的类型，默认为string
}
```

      - 常数列
```
{
  "value": "ffff", // 常数值
  "type": "string" // 常数列的类型，默认为string
}
```

- 必选：否
  <br />如果不指定idColumns属性，则会随机产生文档id
- 默认值：无



- **versionColumn**
    - 描述：指定写入hbase的时间戳。支持：当前时间、指定时间列，指定时间，三者选一。若不配置表示用当前时间。index：指定对应reader端column的索引，从0开始，需保证能转换为long,若是Date类型，会尝试用yyyy-MM-dd HH:mm:ss和yyyy-MM-dd HH:mm:ss SSS去解析；若不指定index；value：指定时间的值,类型为字符串。配置格式如下：
```
"versionColumn":{
"index":1
}
```

- <br />或者
```
"versionColumn":{
"value":"123456789"
}
```

<br />

<a name="ks2VQ"></a>

### 2、SQL
- **connector**
    - 描述：hbase14-x
    - 必选：是
    - 参数类型：String
    - 默认值：无
      <br />
- **table-name**
    - 描述：表名
    - 必选：是
    - 参数类型：String
    - 默认值：无：
      <br />
- **zookeeper.quorum**
    - 描述：HBase的Zookeeper地址
    - 必选：是
    - 参数类型：String
    - 默认值：无
      <br />
- **zookeeper.znode.parent**
    - 描述：root dir
    - 必选：是
    - 参数类型：String
    - 默认值：/hbase
      <br />

- **null-string-literal**
    - 描述：当字符串值为 null 时的存储形式
    - 必选：是
    - 参数类型：String
    - 默认值：null
      <br />

- **properties.***
    - 描述：HBase原生选项 如'properties.hbase.security.authentication' = 'kerberos'.
    - 必选：是
    - 参数类型：String
    - 默认值：无
      <br />
- **sink.buffer-flush.max-rows**
    - 描述：批量写数据条数，单位：条
    - 必选：否
    - 参数类型：String
    - 默认值：1024
      <br />

- **sink.buffer-flush.interval**
    - 描述：批量写时间间隔，单位：时间
    - 必选：否
    - 参数类型：Duration
    - 默认值：10000
      <br />

- **sink.parallelism**
    - 描述：写入结果的并行度
    - 必选：否
    - 参数类型：String
    - 默认值：无
      <br />

- **security.kerberos.principal**
    - 描述：kerberos认证的principal
    - 必选：是
    - 默认值：无
- **security.kerberos.keytab**
    - 描述：kerberos认证的keytab文件路径
    - 必选：是
    - 默认值：无
- **security.kerberos.krb5conf**
    - 描述：kerberos认证的krb5conf文件路径
    - 必选：是
    - 默认值：无

## 五、数据类型
| 支持 | BOOLEAN、TINYINT、SMALLINT、INT、BIGINT、FLOAT、DOUBLE、DECIMAL、STRING、VARCHAR、CHAR、TIMESTAMP、DATE、BINARY |
| --- | --- |
| 暂不支持 | ARRAY、MAP、STRUCT、UNION |


## 六、脚本示例
见项目内`flinkx-examples`文件夹。
