# HDFS读取插件（hdfsreader）

## 1. 配置样例

```
{
    "job": {
        "content": [{
            "reader": {
                "parameter": {
                    "path": "hdfs://ns1/user/hive/warehouse/wujing_test.db/test",
                    "hadoopConfig": {
                        "dfs.ha.namenodes.ns1": "nn1,nn2",
                        "dfs.namenode.rpc-address.ns1.nn2": "node03:9000",
                        "dfs.client.failover.proxy.provider.ns1": "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider",
                        "dfs.namenode.rpc-address.ns1.nn1": "node02:9000",
                        "dfs.nameservices": "ns1"
                    },
                    "defaultFS": "hdfs://ns1",
                    "column": [{
                        "name": "col1",
                        "index": 0,
                        "type": "string",
                        "value": "",
                        "format": ""
                    }],
                    "fieldDelimiter": "",
                    "encoding": "utf-8",
                    "fileType": "orc"
                },
                "name": "hdfsreader"
            },
            "writer": {}
        }],
        "setting": {}
    }
}
```

## 2. 参数说明

* **path**
  
  * 描述：要读取的文件路径，多个路径可以用逗号隔开
  
  * 必选：是 <br />
  
  * 默认值：无 <br />

* **defaultFS**
  
  * 描述：Hadoop hdfs文件系统namenode节点地址。 <br />
  
  * 必选：是 <br />
  
  * 默认值：无 <br />

* **fileType**
  
  * 描述：文件的类型，目前只支持用户配置为"text"、"orc"、“parquet”
    
    * text：textfile文件格式
    
    * orc：orcfile文件格式
    
    * parquet：parquet文件格式
  
  * 必选：是 <br />
  
  * 默认值：无 <br />

* **column**
  
  * 描述：需要读取的字段。
  
  * 格式：支持3中格式
    
    1.读取全部字段，如果字段数量很多，可以使用下面的写法：
    
    ```
    "column":[*]
    ```
    
    2.只指定字段名称：
    
    ```
    "column":["id","name"]
    ```
    
    3.指定具体信息：
    
    ```
    "column": [{
        "name": "col",
        "type": "datetime",
        "format": "yyyy-MM-dd hh:mm:ss",
        "value": "value"
    }]
    ```
  
  * 属性说明:
    
    * name：字段名称
    
    * index：字段索引，当读取text格式的文件时指定此属性
    
    * type：字段类型，可以和数据文件里的字段类型不一样，程序会做一次类型转换
    
    * format：如果字段是时间字符串，可以指定时间的格式，将字段类型转为日期格式返回
    
    * value：如果数据文件里不存在指定的字段，则会把value的值作为常量列返回，如果指定的字段存在，当指定字段的值为null时，会以此value值作为默认值返回

* **fieldDelimiter**
  
  * 描述：读取的字段分隔符 <br />
  
  * 注意：在读取text格式文件时需要指定此参数
  
  * 必选：否 <br />
  
  * 默认值：“\001” <br />

* **encoding**
  
  * 描述：读取文件的编码配置。
  * 必选：否
  * 默认值：utf-8

* **hadoopConfig**
  
  * 描述：hadoopConfig里可以配置与Hadoop相关的一些高级参数，比如HA的配置。<br />
    
    ```
                  "dfs.nameservices": "testDfs",
                  "dfs.ha.namenodes.testDfs": "namenode1,namenode2",
                  "dfs.namenode.rpc-address.aliDfs.namenode1": "",
                  "dfs.namenode.rpc-address.aliDfs.namenode2": "",
                  "dfs.client.failover.proxy.provider.testDfs": "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
          }
    ```
  
  * 必选：否 <br />
  
  * 默认值：无
