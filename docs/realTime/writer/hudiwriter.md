# Hudi Writer

<!-- TOC -->

- [一、插件名称](#一插件名称)
- [二、参数说明](#二参数说明)
- [三、配置示例](#三配置示例)
    - [1、kafka2hudi](#1kafka2hudi)

<!-- /TOC -->

<br/>

## 一、插件名称

**名称：hudiwriter**<br />

<br/>

## 二、参数说明

- **batchInterval**
    - 描述：单次批量写入数据条数，建议配置大于1
    - 必选：否
    - 字段类型：int
    - 默认值：1

<br />

- **tableName**
    - 描述：库表名
    - 必选：是
    - 字段类型：String
    - 默认值：无
    - 注意：英文点号分隔的 {Database}.{Table}

<br />

- **path**
    - 描述：表所在HDFS路径
    - 必选：是
    - 字段类型：String
    - 默认值：无

<br />

- **defaultFS**
    - 描述：Hadoop hdfs文件系统namenode节点地址。格式：hdfs://ip:端口；例如：hdfs://127.0.0.1:9
    - 必选：是
    - 字段类型：String
    - 默认值：无

<br/>

- **hadoopConfig**
    - 描述：集群HA模式时需要填写的namespace配置及其它配置
    - 必选：否
    - 字段类型：Map
    - 默认值：无

<br />

- **column**
    - 描述：目的表需要写入数据的字段,字段之间用英文逗号分隔。例如: "column": ["id","name","age"]。
    - 必选：是
    - 默认值：无
    - 字段类型：List

<br />

- **hiveJdbcUrl**
    - 描述：Hive jdbc链接地址，例如jdbc:hive2://127.0.0.1:9093
    - 必选：是
    - 默认值：无
    - 字段类型：String

<br />

- **hiveMetastore**
    - 描述：Hive Metastore元数据地址，例如thrift://127.0.0.1:9083
    - 必选：是
    - 默认值：无
    - 字段类型：String

<br />

- **hiveUser**
    - 描述：Hive用户名
    - 必选：否
    - 默认值：无
    - 字段类型：String

- **hivePass**
    - 描述：Hive用户对应密码
    - 必选：否
    - 默认值：无
    - 字段类型：String

## 三、配置示例

### 1、kafka2hudi

```json
{
  "job": {
    "content": [
      {
        "reader": {
          "name": "kafkareader",
          "parameter": {
            "blankIgnore": true,
            "codec": "JSON",
            "consumerSettings": {
              "bootstrap.servers": "100.100.100.1:6667,100.100.100.2:6667"
            },
            "groupId": "flink",
            "metaColumns": [
              {
                "name": "id",
                "type": "int"
              },
              {
                "name": "name",
                "type": "string"
              },
              {
                "name": "age",
                "type": "int"
              }
            ],
            "mode": "latest-offset",
            "topic": "flinkx01"
          }
        },
        "writer": {
          "name": "hudiwriter",
          "parameter": {
            "batchInterval": 10,
            "column": [
              {
                "name": "id",
                "type": "int"
              },
              {
                "name": "name",
                "type": "string"
              },
              {
                "name": "age",
                "type": "int"
              }
            ],
            "defaultFS": "hdfs://ns1",
            "hadoopConfig": {
              "dfs.nameservices": "ns1",
              "dfs.ha.namenodes.ns1": "nn1,nn2",
              "dfs.client.failover.proxy.provider.ns1": "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider",
              "dfs.namenode.rpc-address.ns1.nn2": "100.100.100.1:8020",
              "dfs.namenode.rpc-address.ns1.nn1": "100.100.100.2:8020",
              "fs.file.impl": "org.apache.hadoop.fs.LocalFileSystem",
              "fs.hdfs.impl": "org.apache.hadoop.hdfs.DistributedFileSystem",
              "HADOOP_USER_NAME": "hive"
            },
            "hiveJdbcUrl": "jdbc:hive2://localhost:9083",
            "hiveMetastore": "thrift://localhost:9083",
            "hiveUser": "hive",
            "path": "hdfs://ns1/spark_1/lakehouse",
            "tableName": "test.flinkx01"
          }
        }
      }
    ],
    "setting": {
      "speed": {
        "bytes": 0,
        "channel": 1
      }
    }
  }
}
```
