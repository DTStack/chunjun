# Hive写入插件（hivewriter）

## 1. 配置样例

```json
{
    "job": {
        "content": [
            {
                "reader": {

                },
               "writer": {
                   "parameter": {
                       "hadoopConfig": {
                           "dfs.ha.namenodes.ns1" : "nn1,nn2",
                           "fs.defaultFS" : "hdfs://ns1",
                           "dfs.namenode.rpc-address.ns1.nn2" : "node002:9000",
                           "dfs.client.failover.proxy.provider.ns1" : "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider",
                           "dfs.namenode.rpc-address.ns1.nn1" : "node001:9000",
                           "dfs.nameservices" : "ns1",
                           "fs.hdfs.impl.disable.cache" : "true",
                           "fs.hdfs.impl" : "org.apache.hadoop.hdfs.DistributedFileSystem"
                       },

                       "fieldDelimiter": "\u0001",
                       "encoding": "utf-8",
                       "fileType": "orc",

                       "partitionType" : "MINUTE",
                       "partition" : "pt",

                       "writeMode" : "append",

                       "analyticalRules" : "stream_${schema}_${table}_flinkxtest",
                       "password" : "",
                       "tablesColumn" : "{\"date_test\":[{\"type\":\"INT\",\"key\":\"before_id\",\"comment\":\"\"},{\"comment\":\"\",\"type\":\"INT\",\"key\":\"after_id\"},{\"type\":\"DATETIME\",\"key\":\"before_datetime1\",\"comment\":\"\"},{\"comment\":\"\",\"type\":\"DATETIME\",\"key\":\"after_datetime1\"},{\"type\":\"TIMESTAMP\",\"key\":\"before_timestamp\",\"comment\":\"\"},{\"comment\":\"\",\"type\":\"TIMESTAMP\",\"key\":\"after_timestamp\"},{\"comment\":\"\",\"type\":\"varchar\",\"key\":\"type\"},{\"comment\":\"\",\"type\":\"varchar\",\"key\":\"schema\"},{\"comment\":\"\",\"type\":\"varchar\",\"key\":\"table\"},{\"comment\":\"\",\"type\":\"bigint\",\"key\":\"ts\"}]}",
                       "jdbcUrl" : "jdbc:hive2://node001:10000/data_map",

                       "charsetName" : "utf-8",
                       "username" : ""
                   },
                   "name": "hivewriter"
                }
            }
        ]
    }
}
```

## 2. 参数说明

* **name**
  
  * 描述：插件名称hivewriter，hivewriter一般结合mysql binlog插件使用，hive插件底层使用的是hdfswriter插件的共，所以需要填写hdfswriter插件需要的参数。hivewriter插件支持同时写入多张表的多个分区，以及自动创建hive表。开启kerberos的话参考文档[数据源开启Kerberos](kerberos.md)。
  
  * 必选：是
  
  * 默认值：无

* **partitionType**
  
  * 描述：分区类型，包括 DAY、HOUR、MINUTE三种
    
    * DAY：天分区
    
    * HOUR：小时分区
    
    * MINUTE：分钟分区
  
  * 必选：是
  
  * 默认值：无

* **analyticalRules**
  
  * 描述：表名映射规则。以“stream_\${schema}_\${table}_flinkxtest”为列，创建表时会将规则中的schema和table替换
  
  * 必选：否
  
  * 默认值：无

* **tablesColumn**
  
  * 描述：写入hive表的表结构信息，示例：
    
    ```json
    {
        "date_test": [ //表名
    
            {
                "type": "INT",
                "key": "before_id",
                "comment": ""
            },
            {
                "comment": "",
                "type": "INT",
                "key": "after_id"
            }
        ]
    }
    ```
  
  * 必选：是
  
  * 默认值：无
