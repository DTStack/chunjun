# Hive Writer

<a name="c6v6n"></a>
## 一、插件名称
名称：**hivewriter**<br />
<a name="jVb3v"></a>
## 二、支持的数据源版本
**Hive 2.X**<br />
<a name="2lzA4"></a>
## 三、参数说明<br />

- **jdbcUrl**
  - 描述：连接Hive JDBC的字符串
  - 必选：是
  - 默认值：无



- **username**
  - 描述：Hive认证用户名
  - 必选：否
  - 默认值：无



- **password**
  - 描述：Hive认证密码
  - 必选：否
  - 默认值：无



- **fileType**
  - 描述：文件的类型，目前只支持用户配置为`text`、`orc`、`parquet`
    - text：textfile文件格式
    - orc：orcfile文件格式
    - parquet：parquet文件格式
  - 必选：是
  - 默认值：无



- **fieldDelimiter**
  - 描述：hivewriter中`fileType`为`text`时字段的分隔符，
  - 注意：用户需要保证与创建的Hive表的字段分隔符一致，否则无法在Hive表中查到数据
  - 必选：是
  - 默认值：`\u0001`



- **writeMode**
  - 描述：hivewriter写入前数据清理处理模式：
    - append：追加
    - overwrite：覆盖
  - 注意：overwrite模式时会删除Hive当前分区下的所有文件
  - 必选：否
  - 默认值：append



- **compress**
  - 描述：hdfs文件压缩类型，默认不填写意味着没有压缩
    - text：支持`GZIP`、`BZIP2`格式
    - orc：支持`SNAPPY`、`GZIP`、`BZIP`、`LZ4`格式
    - parquet：支持`SNAPPY`、`GZIP`、`LZO`
  - 注意：`SNAPPY`格式需要用户安装**SnappyCodec**
  - 必选：否
  - 默认值：无



- **charsetName**
  - 描述：写入text文件的编码配置
  - 必选：否
  - 默认值：UTF-8



- **maxFileSize**
  - 描述：写入hdfs单个文件最大大小，单位字节
  - 必须：否
  - 默认值：1073741824‬（1G）



- **tablesColumn**
  - 描述：写入hive表的表结构信息，**若表不存在则会自动建表**。示例：
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

  - 必选：是
  - 默认值：无



- **partition**
  - 描述：分区字段名称
  - 必选：是
  - 默认值：`pt`



- **partitionType**
  - 描述：分区类型，包括 DAY、HOUR、MINUTE三种。**若分区不存在则会自动创建，自动创建的分区时间以当前任务运行的服务器时间为准**
    - DAY：天分区，分区示例：pt=202000101
    - HOUR：小时分区，分区示例：pt=2020010110
    - MINUTE：分钟分区，分区示例：pt=202001011027
  - 必选：是
  - 默认值：无



- **defaultFS**
  - 描述：Hadoop hdfs文件系统namenode节点地址。格式：hdfs://ip:端口；例如：hdfs://127.0.0.1:9
  - 必选：是
  - 默认值：无



- **hadoopConfig**
  - 描述：集群HA模式时需要填写的namespace配置及其它配置
  - 必选：否
  - 默认值：无



<a name="1LBc2"></a>
## 四、配置示例
<a name="v4XEg"></a>
#### 1、写入text
```json
{
  "job": {
    "content": [
      {
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
            "sliceRecordCount": ["100"]
          }
        },
        "writer": {
          "name" : "hivewriter",
          "parameter" : {
            "jdbcUrl" : "jdbc:hive2://kudu3:10000/tudou",
            "username" : "",
            "password" : "",
            "fileType" : "text",
            "fieldDelimiter" : "\u0001",
            "writeMode" : "overwrite",
            "compress" : "",
            "charsetName" : "UTF-8",
            "maxFileSize" : 1073741824,
            "tablesColumn" : "{\"kudu\":[{\"key\":\"id\",\"type\":\"int\"},{\"key\":\"user_id\",\"type\":\"int\"},{\"key\":\"name\",\"type\":\"string\"}]}",
            "partition" : "pt",
            "partitionType" : "DAY",
            "defaultFS" : "hdfs://ns1",
            "hadoopConfig" : {
              "dfs.ha.namenodes.ns1" : "nn1,nn2",
              "dfs.namenode.rpc-address.ns1.nn2" : "kudu2:9000",
              "dfs.client.failover.proxy.provider.ns1" : "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider",
              "dfs.namenode.rpc-address.ns1.nn1" : "kudu1:9000",
              "dfs.nameservices" : "ns1",
              "fs.hdfs.impl.disable.cache" : "true",
              "fs.hdfs.impl" : "org.apache.hadoop.hdfs.DistributedFileSystem"
            }
          }
        }
      }
    ],
    "setting": {
      "restore": {
        "maxRowNumForCheckpoint": 0,
        "isRestore": false,
        "restoreColumnName": "",
        "restoreColumnIndex": 0
      },
      "errorLimit": {
        "record": 0,
        "percentage": 0
      },
      "speed": {
        "bytes": 1048576,
        "channel": 1
      }
    }
  }
}
```
<a name="sacJj"></a>
#### 2、写入orc
```json
{
  "job": {
    "content": [
      {
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
            "sliceRecordCount": ["100"]
          }
        },
        "writer": {
          "name" : "hivewriter",
          "parameter" : {
            "jdbcUrl" : "jdbc:hive2://kudu3:10000/tudou",
            "username" : "",
            "password" : "",
            "fileType" : "orc",
            "fieldDelimiter" : "",
            "writeMode" : "overwrite",
            "compress" : "GZIP",
            "charsetName" : "UTF-8",
            "maxFileSize" : 1073741824,
            "tablesColumn" : "{\"kudu\":[{\"key\":\"id\",\"type\":\"int\"},{\"key\":\"user_id\",\"type\":\"int\"},{\"key\":\"name\",\"type\":\"string\"}]}",
            "partition" : "pt",
            "partitionType" : "DAY",
            "defaultFS" : "hdfs://ns1",
            "hadoopConfig" : {
              "dfs.ha.namenodes.ns1" : "nn1,nn2",
              "dfs.namenode.rpc-address.ns1.nn2" : "kudu2:9000",
              "dfs.client.failover.proxy.provider.ns1" : "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider",
              "dfs.namenode.rpc-address.ns1.nn1" : "kudu1:9000",
              "dfs.nameservices" : "ns1",
              "fs.hdfs.impl.disable.cache" : "true",
              "fs.hdfs.impl" : "org.apache.hadoop.hdfs.DistributedFileSystem"
            }
          }
        }
      }
    ],
    "setting": {
      "restore": {
        "maxRowNumForCheckpoint": 0,
        "isRestore": false,
        "restoreColumnName": "",
        "restoreColumnIndex": 0
      },
      "errorLimit": {
        "record": 0,
        "percentage": 0
      },
      "speed": {
        "bytes": 1048576,
        "channel": 1
      }
    }
  }
}
```
<a name="9b08u"></a>
#### 3、写入parquet
```json
{
  "job": {
    "content": [
      {
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
            "sliceRecordCount": ["100"]
          }
        },
        "writer": {
          "name" : "hivewriter",
          "parameter" : {
            "jdbcUrl" : "jdbc:hive2://kudu3:10000/tudou",
            "username" : "",
            "password" : "",
            "fileType" : "parquet",
            "fieldDelimiter" : "",
            "writeMode" : "overwrite",
            "compress" : "SNAPPY",
            "charsetName" : "UTF-8",
            "maxFileSize" : 1073741824,
            "tablesColumn" : "{\"kudu\":[{\"key\":\"id\",\"type\":\"int\"},{\"key\":\"user_id\",\"type\":\"int\"},{\"key\":\"name\",\"type\":\"string\"}]}",
            "partition" : "pt",
            "partitionType" : "DAY",
            "defaultFS" : "hdfs://ns1",
            "hadoopConfig" : {
              "dfs.ha.namenodes.ns1" : "nn1,nn2",
              "dfs.namenode.rpc-address.ns1.nn2" : "kudu2:9000",
              "dfs.client.failover.proxy.provider.ns1" : "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider",
              "dfs.namenode.rpc-address.ns1.nn1" : "kudu1:9000",
              "dfs.nameservices" : "ns1",
              "fs.hdfs.impl.disable.cache" : "true",
              "fs.hdfs.impl" : "org.apache.hadoop.hdfs.DistributedFileSystem"
            }
          }
        }
      }
    ],
    "setting": {
      "restore": {
        "maxRowNumForCheckpoint": 0,
        "isRestore": false,
        "restoreColumnName": "",
        "restoreColumnIndex": 0
      },
      "errorLimit": {
        "record": 0,
        "percentage": 0
      },
      "speed": {
        "bytes": 1048576,
        "channel": 1
      }
    }
  }
}
```
