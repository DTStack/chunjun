# HDFS Writer

## 一、插件名称
名称：**hdfswriter**


## 二、数据源版本
| 协议 | 是否支持 |
| --- | --- |
| Hadoop 2.x | 支持 |
| Hadoop 3.x | 支持 |



## 三、数据源配置
单机模式：[地址](http://hadoop.apache.org/docs/r2.7.6/hadoop-project-dist/hadoop-common/SingleCluster.html)
集群模式：[地址](http://hadoop.apache.org/docs/r2.7.6/hadoop-project-dist/hadoop-common/ClusterSetup.html)


## 四、参数说明

- **defaultFS**
  - 描述：Hadoop hdfs文件系统namenode节点地址。格式：hdfs://ip:端口；例如：hdfs://127.0.0.1:9
  - 必选：是
  - 字段类型：string
  - 默认值：无

<br/>

- **hadoopConfig**
  - 描述：集群HA模式时需要填写的namespace配置及其它配置
  - 必选：否
  - 字段类型：map
  - 默认值：无

<br/>

- **fileType**
  - 描述：文件的类型，目前只支持用户配置为`text`、`orc`、`parquet`
    - text：textfile文件格式
    - orc：orcfile文件格式
    - parquet：parquet文件格式
  - 必选：是
  - 字段类型：string
  - 默认值：无


<br/>

- **path**
  - 描述：数据文件的路径
  - 必选：是
  - 字段类型：string
  - 默认值：无

<br/>

- **fileName**
  - 描述：写入的目录名称
  - 注意：不为空，写入的路径为 path+fileName
  - 必须：否
  - 字段类型：string
  - 默认值：无

<br/>

- **filterRegex**
  - 描述：文件过滤正则表达式
  - 必选：否
  - 字段类型：string
  - 默认值：无

<br/>

- **fieldDelimiter**
  - 描述：`fileType`为`text`时字段的分隔符
  - 必选：否
  - 字段类型：string
  - 默认值：`\001`

<br/>

- **encoding**
  - 描述：`fileType`为`text`时可配置编码格式
  - 必选：否
  - 字段类型：string
  - 默认值：UTF-8

<br/>

- **maxFileSize**
  - 描述：写入hdfs单个文件最大大小，单位字节
  - 必须：否
  - 字段类型：long
  - 默认值：1073741824‬（1G）

<br/>

- **compress**
  - 描述：hdfs文件压缩类型
    - text：支持`GZIP`、`BZIP2`格式
    - orc：支持`SNAPPY`、`ZLIB`、`LZO`格式
    - parquet：支持`SNAPPY`、`GZIP`、`LZO`格式
  - 注意：`SNAPPY`格式需要用户安装**SnappyCodec**
  - 必选：否
  - 字段类型：string
  - 默认值：
    - text 默认 不进行压缩
    - orc 默认为ZLIB格式
    - parquet 默认为SNAPPY格式

<br/>

- **writeMode**
  - 描述：hdfswriter写入前数据清理处理模式：
    - append：追加
    - overwrite：覆盖
  - 注意：overwrite模式时会删除hdfs当前目录下的所有文件
  - 必选：否
  - 字段类型：string
  - 默认值：append

<br/>

- **column**
  - 描述：需要读取的字段。
  - 格式：指定具体信息：
```json
"column": [{
    "name": "col",
    "type": "datetime"
}]
```

- 属性说明:
  - name：字段名称
  - type：字段类型，可以和源字段类型不一样，程序会做一次类型转换
- 必选：是
- 默认值：无

<br/>

- **fullColumnName**
  - 描述：写入的字段名称
  - 必须：否
  - 字段类型：list
  - 默认值：column的name集合

<br/>

- **fullColumnType**
  - 描述：写入的字段类型
  - 必须：否
  - 字段类型：list
  - 默认值：column的type集合

<br/>

- **rowGroupSIze**
  - 描述：parquet类型文件参数，指定row group的大小，单位字节
  - 必须：否
  - 字段类型：int
  - 默认值：134217728（128M）

<br/>

- **enableDictionary**
  - 描述：parquet类型文件参数，是否启动字典编码
  - 必须：否
  - 字段类型：boolean
  - 默认值：true

## 五、使用示例
#### 1、写入text文件
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
                "name": "name",
                "type": "string"
              }
            ],
            "sliceRecordCount" : ["100"]
          }
        },
        "writer": {
          "parameter": {
            "path": "hdfs://ns1/user/hive/warehouse/dev.db/test_text",
            "fileName": "pt=20201214",
            "column": [
              {
                "name": "id",
                "index": 0,
                "type": "bigint"
              },
              {
                "name": "name",
                "index": 1,
                "type": "string"
              }
            ],
            "writeMode": "overwrite",
            "fieldDelimiter": "\u0001",
            "encoding": "utf-8",
            "defaultFS": "hdfs://ns1",
            "hadoopConfig": {
              "dfs.ha.namenodes.ns1" : "nn1,nn2",
              "dfs.namenode.rpc-address.ns1.nn2" : "host1:9000",
              "dfs.client.failover.proxy.provider.ns1" : "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider",
              "dfs.namenode.rpc-address.ns1.nn1" : "host2:9000",
              "dfs.nameservices" : "ns1"
            },
            "fileType": "text"
          },
          "name": "hdfswriter"
        }
      }
    ],
    "setting": {
      "restore": {
        "isRestore": false
      }
    }
  }
}
```
#### 2、写入orc文件
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
                "name": "name",
                "type": "string"
              }
            ],
            "sliceRecordCount" : ["100"]
          }
        },
        "writer": {
          "parameter": {
            "path": "hdfs://ns1/user/hive/warehouse/dev.db/test_orc",
            "fileName": "pt=20201214",
            "column": [
              {
                "name": "id",
                "index": 0,
                "type": "bigint"
              },
              {
                "name": "name",
                "index": 1,
                "type": "string"
              }
            ],
            "writeMode": "overwrite",
            "fieldDelimiter": "\u0001",
            "encoding": "utf-8",
            "defaultFS": "hdfs://ns1",
            "hadoopConfig": {
              "dfs.ha.namenodes.ns1" : "nn1,nn2",
              "dfs.namenode.rpc-address.ns1.nn2" : "host1:9000",
              "dfs.client.failover.proxy.provider.ns1" : "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider",
              "dfs.namenode.rpc-address.ns1.nn1" : "host2:9000",
              "dfs.nameservices" : "ns1"
            },
            "fileType": "orc"
          },
          "name": "hdfswriter"
        }
      }
    ],
    "setting": {
      "restore": {
        "isRestore": false
      }
    }
  }
}
```
#### 3、写入parquet文件
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
                "name": "name",
                "type": "string"
              }
            ],
            "sliceRecordCount" : ["100"]
          }
        },
        "writer": {
          "parameter": {
            "path": "hdfs://ns1/user/hive/warehouse/dev.db/test_parquet",
            "fileName": "pt=20201214",
            "column": [
              {
                "name": "id",
                "index": 0,
                "type": "bigint"
              },
              {
                "name": "name",
                "index": 1,
                "type": "string"
              }
            ],
            "writeMode": "overwrite",
            "fieldDelimiter": "\u0001",
            "encoding": "utf-8",
            "defaultFS": "hdfs://ns1",
            "hadoopConfig": {
              "dfs.ha.namenodes.ns1" : "nn1,nn2",
              "dfs.namenode.rpc-address.ns1.nn2" : "host1:9000",
              "dfs.client.failover.proxy.provider.ns1" : "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider",
              "dfs.namenode.rpc-address.ns1.nn1" : "host2:9000",
              "dfs.nameservices" : "ns1"
            },
            "fileType": "parquet"
          },
          "name": "hdfswriter"
        }
      }
    ],
    "setting": {
      "restore": {
        "isRestore": false
      }
    }
  }
}
```


