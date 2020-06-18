# Carbondata Reader

<a name="c6v6n"></a>
## 一、插件名称
名称：**carbondatareader**<br />**
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



- **filter**
  - 描述：简单过滤器，目前只支持单条件的简单过滤，形式为 col op value，col为列名；op为关系运算符，包括=,>,>=,<,<=；
value为字面值，如1234， "ssss"
  - 必选：否
  - 默认值：无



- **column**
  - 描述：所配置的表中需要同步的字段集合。<br />
字段包括表字段和常量字段，


表字段的格式：
```
{
	"name": "col1",
	"type": "string"
}
```

    - name：字段名称
    - type：字段类型，可以和数据库里的字段类型不一样，程序会做一次类型转换
    - value：如果数据库里不存在指定的字段，则会把value的值作为常量列返回，如果指定的字段存在，当指定字段的值为null时，会以此value值作为默认值返回
  - 必选：是
  - 默认值：无



- **hadoopConfig**
  - 描述：集群HA模式时需要填写的namespace配置及其它配置
  - 必选：是
  - 默认值：无



- **defaultFS**
  - 描述：Hadoop hdfs文件系统namenode节点地址。
  - 必选：是
  - 默认值：无



<a name="csl6T"></a>
## 四、使用示例
```json
{
  "job": {
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
          "parameter": {
            "print": true
          },
          "name": "streamwriter"
        }
      }
    ],
    "setting": {
      "restore": {
        "maxRowNumForCheckpoint": 0,
        "isRestore": false,
        "restoreColumnName": "",
        "restoreColumnIndex": 0
      },
      "errorLimit": {
        "record": 100
      },
      "speed": {
        "bytes": 0,
        "channel": 1
      },
      "log": {
        "isLogger": false,
        "level": "debug",
        "path": "",
        "pattern": ""
      }
    }
  }
}
```
<a name="qPZ48"></a>
## 
