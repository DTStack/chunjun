# HBase Writer

<a name="saR5B"></a>
## 一、插件名称
名称：**hbasewriter**<br />
<a name="jVb3v"></a>
## 二、支持的数据源版本
**HBase 1.2及以上**<br />
<a name="pFGbG"></a>
## 三、参数说明

- **tablename**
  - 描述：hbase表名
  - 必选：是
  - 字段类型：String
  - 默认值：无
  <br />

    - kerberos配置
    在hbaseConfig中加入以下三条中的任一条即表明开启Kerberos配置：  
    ```
        "hbase.security.authentication" ："Kerberos",
        "hbase.security.authorization" : "Kerberos",
        "hbase.security.auth.enable" : true
    ```
    在开启kerberos后，需要根据自己的集群指定以下两个principal的value值  
    ```
        "hbase.regionserver.kerberos.principal":"hbase/_HOST@DTSTACK.COM",
        "hbase.master.kerberos.principal":"hbase/_HOST@DTSTACK.COM"
    ```
    还需要指定Kerberos相关文件的位置  
    ```
        "principalFile": "path of keytab",
        "java.security.krb5.conf": "path of krb5.conf"
    ```
  - 必选：是
  - 字段类型：Map
  - 默认值：无

  <br />

- **nullMode**
  - 描述：读取的null值时，如何处理。支持两种方式：
    - skip：表示不向hbase写这列；
    - empty：写入HConstants.EMPTY_BYTE_ARRAY，即new byte [0]
  - 必选：否
  - 字段类型：String
  - 默认值：skip
  
  <br />

- **encoding**
  - 描述：字符编码
  - 必选：无
  - 字段类型：String
  - 默认值：UTF-8
  
  <br />

- **walFlag**
  - 描述：在HBase client向集群中的RegionServer提交数据时（Put/Delete操作），首先会先写WAL（Write Ahead Log）日志（即HLog，一个RegionServer上的所有Region共享一个HLog），只有当WAL日志写成功后，再接着写MemStore，然后客户端被通知提交数据成功；如果写WAL日志失败，客户端则被通知提交失败。关闭（false）放弃写WAL日志，从而提高数据写入的性能
  - 必选：否
  - 字段类型：Boolean
  - 默认值：false
  
  <br />

- **writeBufferSize**
  - 描述：设置HBae client的写buffer大小，单位字节。配合autoflush使用。autoflush，开启（true）表示Hbase client在写的时候有一条put就执行一次更新；关闭（false），表示Hbase client在写的时候只有当put填满客户端写缓存时，才实际向HBase服务端发起写请求
  - 必选：否
  - 字段类型：long
  - 默认值：8388608（8M）
  
  <br />

- **column**
  - 描述：要写入的hbase字段。
    - name：指定写入的hbase列，必须为 列族:列名 的格式；
    - type：指定源数据的类型，format指定日期类型的格式
  - 必选：是
  - 字段类型：List
  - 默认值：无
  
  <br />

- **rowkeyColumn**
  - 描述：用于构造rowkey的描述信息，每列形式如下  
  字符串格式为：$(cf:col)，可以多个字段组合：$(cf:col1)_$(cf:col2)，  
  可以使用md5函数：md5($(cf:col))
  - 必选: 是
  - 字段类型：String
  - 默认值：无
  
  <br />

- **versionColumn**
  - 描述：指定写入hbase的时间戳。支持：当前时间、指定时间列，指定时间，三者选一。若不配置表示用当前时间。index：指定对应reader端column的索引，从0开始，需保证能转换为long,若是Date类型，会尝试用yyyy-MM-dd HH:mm:ss和yyyy-MM-dd HH:mm:ss SSS去解析；若不指定index；value：指定时间的值,类型为字符串。注意，在hbase中查询默认会显示时间戳最大的数据，因此简单查询可能会出现看不到更新的情况，需要加过滤条件筛选。  
  配置格式如下：
    ```
    "versionColumn":{
    "index":1
    }
    ```
    或者  
    ```
    "versionColumn":{
    "value":"123456789"
    }
    ```
  - 必选: 否
  - 字段类型：Map
  - 默认值：当前时间

<br />

<a name="ks2VQ"></a>
## 三、配置示例

未开启Kerberos的情况
```
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
开启了Kerberos的情况
```
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
            "zookeeper.znode.parent": "/hbase",
            "hbase.security.auth.enable": true,
            "hbase.regionserver.kerberos.principal":"hbase/host@DTSTACK.COM",
            "hbase.master.kerberos.principal":"hbase/host@DTSTACK.COM",
            "principalFile": "path of keytab",
            "useLocalFile": "true",
            "java.security.krb5.conf": "path of krb5.conf"
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
