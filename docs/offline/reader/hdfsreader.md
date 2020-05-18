# HDFS Reader

<a name="c6v6n"></a>
## 一、插件名称
名称：**hdfsreader**<br />

<a name="jVb3v"></a>
## 二、支持的数据源版本
| 协议 | 是否支持 |
| --- | --- |
| Hadoop 2.x | 支持 |
| Hadoop 3.x | 支持 |



<a name="2lzA4"></a>
## 三、数据源配置
单机模式：[地址](http://hadoop.apache.org/docs/r2.7.6/hadoop-project-dist/hadoop-common/SingleCluster.html)<br />集群模式：[地址](http://hadoop.apache.org/docs/r2.7.6/hadoop-project-dist/hadoop-common/ClusterSetup.html)<br />

<a name="1Pix9"></a>
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



<a name="csl6T"></a>
## 五、使用示例
<a name="tCNPA"></a>
#### 1、读取text文件
```json
{
    "job": {
        "content": [
            {
                "reader": {
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
                        "fileType": "text"
                    },
                    "name": "hdfsreader"
                },
                "writer": {
                    "parameter": {},
                    "name": "streamwriter"
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
<a name="CqZWp"></a>
#### 2、过滤文件名称
```json
{
    "job": {
        "content": [
            {
                "reader": {
                    "parameter": {
                        "path": "hdfs://ns1/flinkx/text",
                        "filterRegex" : ".*\\.csv",
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
                        "fileType": "text"
                    },
                    "name": "hdfsreader"
                },
                "writer": {
                    "parameter": {},
                    "name": "streamwriter"
                }
            }
        ],
        "setting": {
            "speed": {
                "bytes": 1048576,
                "channel": 1
            }
        }
    }
}
```


