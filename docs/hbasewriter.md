# HBase写入插件（hbasewriter）

## 1. 配置样例

```json
{
    "job": {
        "setting": {
            "speed": {},
            "content": [{
                "reader": {},
                "writer": {
                    "name": "hbasewriter",
                    "parameter": {
                        "hbaseConfig": {
                            "hbase.zookeeper.property.clientPort": "2181",
                            "hbase.rootdir": "hdfs://ns1/hbase",
                            "hbase.cluster.distributed": "true",
                            "hbase.zookeeper.quorum": "host1,host2,host3",
                            "zookeeper.znode.parent": "/hbase",
                         "hbase.security.authentication":"Kerberos",
 "hbase.security.authorization":true,
 "hbase.master.kerberos.principal":"hbase/node1@TEST.COM",
 "hbase.master.keytab.file":"hbase.keytab",
 "hbase.regionserver.keytab.file":"hbase.keytab",
 "hbase.regionserver.kerberos.principal":"hbase/node1@TEST.COM"
                        },
                        "table": "tableTest",
                        "rowkeyColumn": [{
                                "index": 0,
                                "type": "string"
                            },
                            {
                                "value": "_postfix",
                                "type": "string"
                            }
                        ],
                        "column": [{
                                "name": "cf1:id",
                                "type": "string"
                            },
                            {
                                "name": "cf1:vv",
                                "type": "string"
                            }
                        ]
                    }
                }
            }]
        }
    }
}
```

## 2. 参数说明

* **hbaseConfig**
  
  * 描述：hbase的连接配置，以json的形式组织 (见hbase-site.xml)，开启kerberos的话参考文档[数据源开启Kerberos](kerberos.md)
  
  * 必选：是 
  
  * 默认值：无

* **table**
  
  * 描述：hbase表名
  
  * 必选：是 
  
  * 默认值：无 

* **column**
  
  * 描述：写入hbase表的若干个列，hbase表的每一列由列簇和列名组成，用":"连接
    
    ```
      {
          "name": "cf1:id", // 列簇:列名
          "type": "string" // 列类型
      }
    ```
  
  * 必选：是 
  
  * 默认值：无 

* **rowkeyColumn**
  
  * 描述：用于构造rowkey的描述信息，支持两种格式，每列形式如下
    
    * 字符串格式
      
      字符串格式为：$(cf:col)，可以多个字段组合：\$(cf:col1)_$(cf:col2)，
      
      可以使用md5函数：md5($(cf:col))
    
    * 数组格式
      
      * 普通列
        
        ```
        {
          "index": 0,  // 该列在column属性中的序号，从0开始
          "type": "string" 列的类型，默认为string
        }
        ```
      
      * 常数列
        
        ```
        {
          "value": "ffff", // 常数值
          "type": "string" // 常数列的类型，默认为string
        }
        ```
  
  * 必选：否 
    
      如果不指定idColumns属性，则会随机产生文档id
  
  * 默认值：无 

* **versionColumn**
  
  * 描述：指定写入hbase的时间戳。支持：当前时间、指定时间列，指定时间，三者选一。若不配置表示用当前时间。index：指定对应reader端column的索引，从0开始，需保证能转换为long,若是Date类型，会尝试用yyyy-MM-dd HH:mm:ss和yyyy-MM-dd HH:mm:ss SSS去解析；若不指定index；value：指定时间的值,类型为字符串。配置格式如下：
    
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

* **encoding**
  
  * 描述：字符编码
  
  * 必选：无 
  
  * 默认值：utf-8 

* **nullMode**
  
  * 描述：读取的null值时，如何处理。支持两种方式：
    
    * （1）skip：表示不向hbase写这列；
    
    * （2）empty：写入HConstants.EMPTY_BYTE_ARRAY，即new byte [0] 
  
  * 必选：否
  
  * 默认值：skip    

* **writeBufferSize**
  
  * 描述：设置HBae client的写buffer大小，单位字节。配合autoflush使用。autoflush，开启（true）表示Hbase client在写的时候有一条put就执行一次更新；关闭（false），表示Hbase client在写的时候只有当put填满客户端写缓存时，才实际向HBase服务端发起写请求
  
  * 必选：否
  
  * 默认值：8M

* **walFlag**
  
  * 描述：在HBae client向集群中的RegionServer提交数据时（Put/Delete操作），首先会先写WAL（Write Ahead Log）日志（即HLog，一个RegionServer上的所有Region共享一个HLog），只有当WAL日志写成功后，再接着写MemStore，然后客户端被通知提交数据成功；如果写WAL日志失败，客户端则被通知提交失败。关闭（false）放弃写WAL日志，从而提高数据写入的性能。
  
  * 必选：否
  
  * 默认值：false
