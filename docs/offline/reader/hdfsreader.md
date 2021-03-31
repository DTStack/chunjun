# HDFS Reader

## 一、插件名称
名称：**hdfsreader**


## 二、支持的数据源版本
| 协议 | 是否支持 |
| --- | --- |
| Hadoop 2.x | 支持 |
| Hadoop 3.x | 支持 |



## 三、数据源配置
单机模式：[地址](http://hadoop.apache.org/docs/r2.7.6/hadoop-project-dist/hadoop-common/SingleCluster.html)
集群模式：[地址](http://hadoop.apache.org/docs/r2.7.6/hadoop-project-dist/hadoop-common/ClusterSetup.html)


## 四、参数说明

- **defaultFS**
  - 描述：Hadoop hdfs文件系统namenode节点地址。格式：hdfs://ip:端口；例如：hdfs://127.0.0.1:9
  - 必选：是
  - 参数类型：string
  - 默认值：无

<br/>

- **hadoopConfig**
  - 描述：集群HA模式时需要填写的namespace配置及其它配置
  - 必选：否
  - 参数类型：map
  - 默认值：无

<br/>

- **path**
  - 描述：数据文件的路径
  - 注意：真正读取的文件路径是 path+fileName
  - 必选：是
  - 参数类型：string
  - 默认值：无

<br/>

- **fileName**
  - 描述：数据文件目录名称
  - 注意：不为空，则hdfs读取的路径为 path+filename
  - 必选：否
  - 参数类型：string
  - 默认值：无

<br/>

- **filterRegex**
  - 描述：文件正则表达式,读取匹配到的文件
  - 必选：否
  - 参数类型：string
  - 默认值：无

<br/>

- **fileType**
  - 描述：文件的类型，目前只支持用户配置为`text`、`orc`、`parquet`
    - text：textfile文件格式
    - orc：orcfile文件格式
    - parquet：parquet文件格式
  - 必选：否
  - 参数类型：string
  - 默认值：text

<br/>

- **fieldDelimiter**
  - 描述：`fileType`为`text`时字段的分隔符
  - 必选：否
  - 参数类型：string
  - 默认值：`\001`

<br/>

- **column**
  - 描述：需要读取的字段
  - 注意：不支持*格式
  - 格式：
```json
"column": [{
    "name": "col",
    "type": "datetime",
    "index":1,
    "isPart":false,
    "format": "yyyy-MM-dd hh:mm:ss",
    "value": "value"
}]
```

- 属性说明:
  - name：必选，字段名称
  - type：必选，字段类型，可以和文件里的字段类型不一样，程序会做一次类型转换
  - index：非必选，字段在所有字段里的位置 从0开始计算，默认为-1
  - isPart：非必选，是否是分区字段，如果是分区字段，会自动从path上截取分区赋值，默认为fale
  - format：非必选，如果字段是时间字符串，可以指定时间的格式，将字段类型转为日期格式返回
  - value：非必选，如果文件里不存在指定的字段，则会把value的值作为常量列返回
- 必选：是
- 参数类型：数组
- 默认值：无

## 五、使用示例
#### 1、读取text文件
```json
{
  "job": {
    "content": [
      {   "reader" : {
        "parameter" : {
          "path" : "/user/hive/warehouse/dev.db/merge_text",
          "hadoopConfig" : {
            "dfs.ha.namenodes.ns1" : "nn1,nn2",
            "dfs.namenode.rpc-address.ns1.nn2" : "host1:9000",
            "dfs.client.failover.proxy.provider.ns1" : "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider",
            "dfs.namenode.rpc-address.ns1.nn1" : "host2:9000",
            "dfs.nameservices" : "ns1"
          },
          "column" : [ {
            "name": "col1",
            "index" : 0,
            "type" : "STRING"
          }, {
            "name": "col2",
            "index" : 1,
            "type" : "STRING"
          } ],
          "defaultFS" : "hdfs://ns1",
          "fieldDelimiter" : "\u0001",
          "fileType" : "text"
        },
        "name" : "hdfsreader"
      },
        "writer": {
          "name": "streamwriter",
          "parameter": {
            "print": true
          }
        }
      }
    ],
    "setting": {
      "speed": {
        "channel": 1
      },
      "restore": {
        "isRestore": false
      }
    }
  }
}
```
#### 2、过滤文件名称
```json
{
  "job": {
    "content": [
      {   "reader" : {
        "parameter" : {
          "path" : "/user/hive/warehouse/dev.db/merge_orc",
          "filterRegex" : "..*\\.snappy",
          "hadoopConfig" : {
            "dfs.ha.namenodes.ns1" : "nn1,nn2",
            "dfs.namenode.rpc-address.ns1.nn2" : "host1:9000",
            "dfs.client.failover.proxy.provider.ns1" : "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider",
            "dfs.namenode.rpc-address.ns1.nn1" : "host2:9000",
            "dfs.nameservices" : "ns1"
          },
          "column" : [ {
            "name": "col1",
            "index" : 0,
            "type" : "STRING"
          }, {
            "name": "col2",
            "index" : 1,
            "type" : "STRING"
          } ],
          "defaultFS" : "hdfs://ns1",
          "fileType" : "orc"
        },
        "name" : "hdfsreader"
      },
        "writer": {
          "name": "streamwriter",
          "parameter": {
            "print": true
          }
        }
      }
    ],
    "setting": {
      "speed": {
        "channel": 1
      },
      "restore": {
        "isRestore": false
      }
    }
  }
}
```


