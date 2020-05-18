# HDFS Writer

<a name="gRdrI"></a>
## 一、插件名称
名称：**hdfswriter**<br />

<a name="X11Yw"></a>
## 二、数据源版本
| 协议 | 是否支持 |
| --- | --- |
| Hadoop 2.x | 支持 |
| Hadoop 3.x | 支持 |



<a name="ppqfG"></a>
## 三、数据源配置
单机模式：[地址](http://hadoop.apache.org/docs/r2.7.6/hadoop-project-dist/hadoop-common/SingleCluster.html)<br />集群模式：[地址](http://hadoop.apache.org/docs/r2.7.6/hadoop-project-dist/hadoop-common/ClusterSetup.html)<br />

<a name="n6PbR"></a>
## 四、参数说明

- **defaultFS**
  - 描述：Hadoop hdfs文件系统namenode节点地址。格式：hdfs://ip:端口；例如：hdfs://127.0.0.1:9
  - 必选：是
  - 默认值：无



- **hadoopConfig**
  - 描述：集群HA模式时需要填写的namespace配置及其它配置
  - 必选：否
  - 默认值：无



- **path**
  - 描述：数据文件的路径
  - 必选：是
  - 默认值：无



- **filterRegex**
  - 描述：文件过滤正则表达式
  - 必选：否
  - 默认值：无



- **fileType**
  - 描述：文件的类型，目前只支持用户配置为`text`、`orc`、`parquet`
    - text：textfile文件格式
    - orc：orcfile文件格式
    - parquet：parquet文件格式
  - 必选：否
  - 默认值：text



- **fieldDelimiter**
  - 描述：`fileType`为`text`时字段的分隔符
  - 必选：否
  - 默认值：`\001`



- **encoding**
  - 描述：`fileType`为`text`时可配置编码格式
  - 必选：否
  - 默认值：UTF-8



- **maxFileSize**
  - 描述：写入hdfs单个文件最大大小，单位字节
  - 必须：否
  - 默认值：1073741824‬（1G）



- **compress**
  - 描述：hdfs文件压缩类型，默认不填写意味着没有压缩
    - text：支持`GZIP`、`BZIP2`格式
    - orc：支持`SNAPPY`、`GZIP`、`BZIP`、`LZ4`格式
    - parquet：支持`SNAPPY`、`GZIP`、`LZO`
  - 注意：`SNAPPY`格式需要用户安装**SnappyCodec**
  - 必选：否
  - 默认值：无



- **compress**
  - 描述：hdfs文件压缩类型，默认不填写意味着没有压缩
    - text：支持`GZIP`、`BZIP2`格式
    - orc：支持`SNAPPY`、`GZIP`、`BZIP`、`LZ4`格式
    - parquet：支持`SNAPPY`、`GZIP`、`LZO`
  - 注意：`SNAPPY`格式需要用户安装**SnappyCodec**
  - 必选：否
  - 默认值：无



- **fileName**
  - 描述：写入的目录名称
  - 必须：否
  - 默认值：无



- **writeMode**
  - 描述：hdfswriter写入前数据清理处理模式：
    - append：追加
    - overwrite：覆盖
  - 注意：overwrite模式时会删除hdfs当前目录下的所有文件
  - 必选：否
  - 默认值：append



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
    - type：字段类型，可以和数据库里的字段类型不一样，程序会做一次类型转换
  - 必选：是
  - 默认值：无



<a name="8fMkI"></a>
## 五、使用示例
<a name="opEbj"></a>
#### 1、写入text文件
```json
{
    "job": {
        "content": [
            {
                "reader": {
                    "parameter": {
                        "column": [
                            {
                                "name": "col1",
                                "type": "string"
                            },
                            {
                                "name": "col2",
                                "type": "string"
                            },
                            {
                                "name": "col3",
                                "type": "int"
                            },
                            {
                                "name": "col4",
                                "type": "int"
                            }
                        ],
                        "sliceRecordCount": [
                            "100"
                        ]
                    },
                    "name": "streamreader"
                },
                "writer": {
                    "parameter": {
                        "path": "hdfs://ns1/flinkx/text",
                        "defaultFS": "hdfs://ns1",
                        "hadoopConfig": {
                            "dfs.ha.namenodes.ns1": "nn1,nn2",
                            "dfs.namenode.rpc-address.ns1.nn2": "flinkx02:9000",
                            "dfs.client.failover.proxy.provider.ns1": "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider",
                            "dfs.namenode.rpc-address.ns1.nn1": "flinkx01:9000",
                            "dfs.nameservices": "ns1"
                        },
                        "column": [
                            {
                                "name": "col1",
                                "index": 0,
                                "type": "string"
                            },
                            {
                                "name": "col2",
                                "index": 1,
                                "type": "string"
                            },
                            {
                                "name": "col3",
                                "index": 2,
                                "type": "int"
                            },
                            {
                                "name": "col4",
                                "index": 3,
                                "type": "int"
                            }
                        ],
                        "fieldDelimiter": ",",
                        "fileType": "text",
                        "writeMode": "append"
                    },
                    "name": "hdfswriter"
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
<a name="4rN8Z"></a>
#### 2、写入orc文件
```json
{
    "job": {
        "content": [
            {
                "reader": {
                    "parameter": {
                        "column": [
                            {
                                "name": "col1",
                                "type": "string"
                            },
                            {
                                "name": "col2",
                                "type": "string"
                            },
                            {
                                "name": "col3",
                                "type": "int"
                            },
                            {
                                "name": "col4",
                                "type": "int"
                            }
                        ],
                        "sliceRecordCount": [
                            "100"
                        ]
                    },
                    "name": "streamreader"
                },
                "writer": {
                    "parameter": {
                        "path": "hdfs://ns1/flinkx/text",
                        "defaultFS": "hdfs://ns1",
                        "hadoopConfig": {
                            "dfs.ha.namenodes.ns1": "nn1,nn2",
                            "dfs.namenode.rpc-address.ns1.nn2": "flinkx02:9000",
                            "dfs.client.failover.proxy.provider.ns1": "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider",
                            "dfs.namenode.rpc-address.ns1.nn1": "flinkx01:9000",
                            "dfs.nameservices": "ns1"
                        },
                        "column": [
                            {
                                "name": "col1",
                                "index": 0,
                                "type": "string"
                            },
                            {
                                "name": "col2",
                                "index": 1,
                                "type": "string"
                            },
                            {
                                "name": "col3",
                                "index": 2,
                                "type": "int"
                            },
                            {
                                "name": "col4",
                                "index": 3,
                                "type": "int"
                            }
                        ],
                        "fileType": "orc",
                        "writeMode": "append"
                    },
                    "name": "hdfswriter"
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
<a name="4n3j4"></a>
#### 3、写入parquet文件
```json
{
    "job": {
        "content": [
            {
                "reader": {
                    "parameter": {
                        "column": [
                            {
                                "name": "col1",
                                "type": "string"
                            },
                            {
                                "name": "col2",
                                "type": "string"
                            },
                            {
                                "name": "col3",
                                "type": "int"
                            },
                            {
                                "name": "col4",
                                "type": "int"
                            }
                        ],
                        "sliceRecordCount": [
                            "100"
                        ]
                    },
                    "name": "streamreader"
                },
                "writer": {
                    "parameter": {
                        "path": "hdfs://ns1/flinkx/text",
                        "defaultFS": "hdfs://ns1",
                        "hadoopConfig": {
                            "dfs.ha.namenodes.ns1": "nn1,nn2",
                            "dfs.namenode.rpc-address.ns1.nn2": "flinkx02:9000",
                            "dfs.client.failover.proxy.provider.ns1": "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider",
                            "dfs.namenode.rpc-address.ns1.nn1": "flinkx01:9000",
                            "dfs.nameservices": "ns1"
                        },
                        "column": [
                            {
                                "name": "col1",
                                "index": 0,
                                "type": "string"
                            },
                            {
                                "name": "col2",
                                "index": 1,
                                "type": "string"
                            },
                            {
                                "name": "col3",
                                "index": 2,
                                "type": "int"
                            },
                            {
                                "name": "col4",
                                "index": 3,
                                "type": "int"
                            }
                        ],
                        "fileType": "parquet",
                        "writeMode": "append"
                    },
                    "name": "hdfswriter"
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


