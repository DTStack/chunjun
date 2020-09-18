# Carbondata Writer

<a name="c6v6n"></a>
## 一、插件名称
名称：**carbondatawriter**<br />**
<a name="jVb3v"></a>
## 二、支持的数据源版本
**Carbondata 1.5及以上**<br />

<a name="2lzA4"></a>
## 三、参数说明

- **path**
  - 描述：carbondata表的存储路径
  - 必选：是
  - 默认值：无



- **table**
  - 描述：carbondata表名
  - 必选：否
  - 默认值：无



- **database**
  - 描述：carbondata库名
  - 必选：否
  - 默认值：无



- **column**
  - 描述：所配置的表中需要同步的字段名列表
  - 必选：是
  - 默认值：无



- **hadoopConfig**
  - 描述：集群HA模式时需要填写的namespace配置及其它配置
  - 必选：是
  - 默认值：无

<br />

- **defaultFS**
  - 描述：Hadoop hdfs文件系统namenode节点地址
  - 必选：是
  - 默认值：无



- **writeMode**
  - 描述：写入模式，支持append和overwrite
  - 必填：否
  - 默认值：append



- **partition**
  - 描述：carbondata分区
  - 必填：否
  - 默认值：append



- **batchSize**
  - 描述：批量提交条数
  - 必填：否
  - 默认值：204800



<a name="1LBc2"></a>
## 四、配置示例
```json
{
  "job" : {
    "content" : [ {
      "reader" : {
        "parameter" : {
          "column" : [ {
            "name" : "id",
            "type" : "id"
          }, {
            "name" : "data",
            "type" : "string"
          } ],
          "sliceRecordCount" : [ "100"]
        },
        "name" : "streamreader"
      },
      "writer" : {
        "name": "carbondatawriter",
        "parameter": {
          "path": "hdfs://ns1/user/hive/warehouse/carbon.store1/sb/sb500",
          "hadoopConfig": {
            "dfs.ha.namenodes.ns1": "nn1,nn2",
            "dfs.namenode.rpc-address.ns1.nn2": "rdos2:9000",
            "dfs.client.failover.proxy.provider.ns1": "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider",
            "dfs.namenode.rpc-address.ns1.nn1": "rdos1:9000",
            "dfs.nameservices": "ns1"
          },
          "defaultFS": "hdfs://ns1",
          "table": "sb500",
          "database": "sb",
          "writeMode": "overwrite",
          "column": ["a","b"],
          "batchSize": 204800
        }
    } ],
    "setting" : {
      "restore" : {
        "maxRowNumForCheckpoint" : 0,
        "isRestore" : false,
        "restoreColumnName" : "",
        "restoreColumnIndex" : 0
      },
      "errorLimit" : {
        "record" : 100
      },
      "speed" : {
        "bytes" : 0,
        "channel" : 1
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


