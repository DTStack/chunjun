# HDFS写入插件（hdfswriter）

## 1. 配置样例

```
{
    "job": {
        "setting": {},
        "content": [{
            "reader": {},
            "writer": {
                "name": "hdfswriter",
                "parameter": {
                    "hadoopConfig": {
                        "dfs.nameservices": "ns1",
                        "dfs.ha.namenodes.ns1": "nn1,nn2",
                        "dfs.namenode.rpc-address.ns1.nn1": "node02:9000",
                        "dfs.namenode.rpc-address.ns1.nn2": "node03:9000",
                        "dfs.client.failover.proxy.provider.ns1": "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
                    },
                    "defaultFS": "hdfs://ns1",
                    "fileType": "text",
                    "fileName": "hello",
                    "column": [{
                        "name": "col1",
                        "index": 0,
                        "type": "STRING"
                    }],
                    "rowGroupSize": 134217728,
                    "compress": "SNAPPY",
                    "path": "/test",
                    "writeMode": "append",
                    "fieldDelimiter": "\\001",
                    "maxFileSize":1073741824‬
                }
            }
        }]
    }
}
```

## 2. 参数说明

* **defaultFS**
  
  * 描述：Hadoop hdfs文件系统namenode节点地址。格式：hdfs://ip:端口；例如：hdfs://127.0.0.1:9
  
  * 必选：是 <br />
  
  * 默认值：无 <br />

* **fileType**
  
  * 描述：文件的类型，目前只支持用户配置为"text"、"orc"、“parquet”
    
    * text：textfile文件格式
    
    * orc：orcfile文件格式
    
    * parquet：parquet文件格式
  
  * 必选：是 <br />
  
  * 默认值：无 <br />

* **path**
  
  * 描述：存储到Hadoop hdfs文件系统的路径信息，HdfsWriter会根据并发配置在Path目录下写入多个文件。
  
  * 必选：是 <br />
  
  * 默认值：无 <br />

* **rowGroupSize**
  
  * 描述：写入parquet格式文件时指定，表示一个group的大小，如果字段数量很多，并且任务可使用内存有限，使用默认值可能会导致内存溢出，可以通过降低此参数的值来避免内存溢出，如果值很小，则会生产很多小的group，此时通过hive或者spark处理的话会降低效率，因此这个参数的调整要结合具体使用场景。
  
  * 必选：否
  
  * 默认值：134217728

* **column**
  
  * 描述：写入数据的字段。
    
    * name：指定字段名
    
    * type：指定字段类型。
  
  * 必选：是 <br />
  
  * 默认值：无 <br />

* **writeMode**
  
  * 描述：hdfswriter写入前数据清理处理模式： <br />
    * append，追加
    
    * overwrite，覆盖
  * 注意：overwrite模式时会删除写入路径下的所有文件
  * 必选：否
  * 默认值：overwrite

* **fieldDelimiter**
  
  * 描述：hdfswriter写入时的字段分隔符,**需要用户保证与创建的Hive表的字段分隔符一致，否则无法在Hive表中查到数据** <br />
  
  * 必选：是 <br />
  
  * 默认值：\\001 <br />

* **compress**
  
  * 描述：hdfs文件压缩类型，默认不填写意味着没有压缩。其中：text类型文件支持压缩类型有gzip、bzip2;orc类型文件支持的压缩类型有NONE、SNAPPY（需要用户安装SnappyCodec）。 <br />
  
  * 必选：否 <br />
  
  * 默认值：无压缩 <br />

* **encoding**
  
  * 描述：写文件的编码配置。<br />
  * 必选：否
  * 默认值：utf-8

* **maxFileSize**
  
  * 描述：hdfs文件最大大小，单位字节
  
  * 必须：否
  
  * 默认值：1073741824‬（1G）
