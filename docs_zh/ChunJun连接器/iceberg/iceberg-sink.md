# Iceberg Sink

## 一、介绍
将数据写入数据湖iceberg中, 目前只支持追加模式


## 二、参数说明
| 参数名       | 是否必填 | 默认值 | 类型   | 含义                                                 |
| ------------ | -------- | ------ | ------ | ---------------------------------------------------- |
| warehouse    | 是       | none   | String | hive的路径，示例: (hdfs:///dtInsight/hive/warehouse) |
| uri          | 是       | none   | String | metastore uri                                        |
| hadoopConfig | 是       | none   | map    | hadoop配置                                           |
| database     | 是       | none   | String | iceberg 数据库                                       |
| table        | 是       | none   | String | iceberg 表名                                         |
| writeMode    | 否       | append | String | 写入模式, 支持overwrite

示例: 
```json
{
    "parameter": {
    "column": [ {
        "name": "id",
        "index": 0,
        "resourceName": "",
        "type": "INT",
        "key": "id"
    }],
    "uri": "thrift://172-16-23-238:9083",
    "warehouse": "hdfs:///dtInsight/hive/warehouse",
    "database": "kungen",
    "table": "test1234",
    "hadoopConfig": {
              "fs.defaultFS":"hdfs://ns1",
              "dfs.nameservices":"ns1",
              "dfs.ha.namenodes.ns1":"nn1,nn2",
              "dfs.namenode.rpc-address.ns1.nn1":"172.16.21.107:9000",
              "dfs.namenode.rpc-address.ns1.nn2":"172.16.22.103:9000",
              "dfs.client.failover.proxy.provider.ns1": "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
    }, 
    "writeMode": "overwrite"
  },
  "name": "icebergwriter"
}
```

