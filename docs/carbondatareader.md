# Carbondata读取插件（carbondatareader）

## 1. 配置样例

```
{
    "job": {
        "setting": {
            "speed": {
                 "channel": 3,
                 "bytes": 0
            },
            "errorLimit": {
                "record": 10000,
                "percentage": 100
            }
        },
        "content": [
            {
                "reader": {
                    "name": "carbondatareader",
                    "parameter": {
                        "path": "hdfs://ns1/user/hive/warehouse/carbon.store1/sb/tb2000",
                        "hadoopConfig": {
                            "dfs.ha.namenodes.ns1": "nn1,nn2",
                            "dfs.namenode.rpc-address.ns1.nn2": "rdos2:9000",
                            "dfs.client.failover.proxy.provider.ns1": "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider",
                            "dfs.namenode.rpc-address.ns1.nn1": "rdos1:9000",
                            "dfs.nameservices": "ns1"
                        },
                        "defaultFS": "hdfs://ns1",
                        "table": "tb2000",
                        "database": "sb",
                        "filter": " b = 100",
                        "column": [
                            {
                                "name": "a",
                                "type": "string"
                            },
                            {
                                "name": "b",
                                "type": "int"
                            }
                        ]
                    }
                },
               "writer": {
                    "name": "mysqlwriter",
                    "parameter": {
                        "writeMode": "insert",
                        "username": "root",
                        "password": "111111",
                        "column": [
                            "v",
                            "id"
                        ],
                        "batchSize": 1,
                        "connection": [
                            {
                                "jdbcUrl": "jdbc:mysql://rdos1:3306/hyf",
                                "table": [
                                    "tt2"
                                ]
                            }
                        ]
                    }
                }
            }
        ]
    }
}
```

## 2. 参数说明

* **name**
  
  * 描述：插件名，此处只能填carbondatareader，否则Flinkx将无法正常加载该插件包。
    * 必选：是 <br />
    
    * 默认值：无 <br />

* **path**
  
  * 描述：carbondata表的存储路径
  
  * 必选：是 <br />
  
  * 默认值：无 <br />

* **table**
  
  * 描述：carbondata表名 <br />
  
  * 必选：否 <br />
  
  * 默认值：无 <br />

* **database**
  
  * 描述：carbondata库名 <br />
  
  * 必选：否 <br />
  
  * 默认值：无 <br />

* **filter**
  
  * 描述：简单过滤器，目前只支持单条件的简单过滤，形式为 col op value<br />
    
    col为列名；<br />
    
    op为关系运算符，包括=,>,>=,<,<=；<br />
    
    value为字面值，如1234， "ssss" <br />
  
  * 必选：否 <br />
  
  * 默认值：无 <br />

* **column**
  
  * 描述：所配置的表中需要同步的字段集合。
    
    字段包括表字段和常量字段，<br />
    
    表字段的格式：
    
    ```
    {
      "name": "col1",
      "type": "string"
    }
    ```
    
    常量字段的格式：
    
    {
    
      "value": "12345",
    
      "type": "string"
    
    }

    * 必选：是 <br />
    
    * 默认值：无 <br />

* **hadoopConfig**
  
  * 描述：hadoopConfig里可以配置与Hadoop相关的一些高级参数，比如HA的配置。<br />
    
    ```
      {
      "hadoopConfig": {
                          "dfs.ha.namenodes.ns1": "nn1,nn2",
                          "dfs.namenode.rpc-address.ns1.nn2": "rdos2:9000",
                          "dfs.client.failover.proxy.provider.ns1": "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider",
                          "dfs.namenode.rpc-address.ns1.nn1": "rdos1:9000",
                          "dfs.nameservices": "ns1",
                          "fs.defaultFS": "hdfs://ns1"
                      }
      }
    ```

* **defaultFS**
  
  * 描述：Hadoop hdfs文件系统namenode节点地址。 <br />
  
  * 必选：是 <br />
  
  * 默认值：无 <br />

## 3. 数据类型

支持如下数据类型

* SMALLINT
* INT/INTEGER
* BIGINT
* DOUBLE
* DECIMAL
* FLOAT
* BYTE
* BOOLEAN
* STRING
* CHAR
* VARCHAR
* DATE
* TIMESTAMP

不支持如下数据类型

* arrays: ARRAY<data_type>
* structs: STRUCT<col_name : data_type COMMENT col_comment, ...>
* maps: MAP<primitive_type, data_type>
