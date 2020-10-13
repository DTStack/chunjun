# HBase Writer

<a name="saR5B"></a>
## 一、插件名称
名称：**hbasewriter**<br />
<a name="jVb3v"></a>
## 二、支持的数据源版本
**HBase 1.3及以上**<br />
<a name="pFGbG"></a>
## 三、参数说明

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
## 三、配置示例
```json
{
  "job" : {
    "content" : [ {
      "reader": {
        "name": "streamreader",
        "parameter": {
          "column": [
            {
              "name": "id",
              "type": "id"
            },
            {
              "name": "user_id",
              "type": "int"
            },
            {
              "name": "name",
              "type": "string"
            }
          ],
          "sliceRecordCount" : ["100"]
        }
      },
      "writer": {
        "name": "hbasewriter",
        "parameter": {
          "hbaseConfig": {
            "hbase.zookeeper.property.clientPort": "2181",
            "hbase.rootdir": "hdfs://ns1/hbase",
            "hbase.cluster.distributed": "true",
            "hbase.zookeeper.quorum": "node01,node02,node03",
            "zookeeper.znode.parent": "/hbase"
          },
          "table": "tb1",
          "rowkeyColumn": "col1#col2",
          "column": [
            {
              "name": "cf1:id",
              "type": "int"
            },
            {
              "name": "cf1:user_id",
              "type": "int"
            },
            {
              "name": "cf1:name",
              "type": "string"
            }
          ]
        }
      }
    } ],
    "setting": {
      "speed": {
        "channel": 1,
        "bytes": 0
      },
      "errorLimit": {
        "record": 100
      },
      "restore": {
        "maxRowNumForCheckpoint": 0,
        "isRestore": false,
        "isStream" : false,
        "restoreColumnName": "",
        "restoreColumnIndex": 0
      },
      "log" : {
        "isLogger": false,
        "level" : "debug",
        "path" : "",
        "pattern":""
      }
    }
  }
}
```

<br />

