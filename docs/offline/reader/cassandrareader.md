# Cassandra Reader

<a name="s9KC6"></a>
## 一、插件名称
**名称：cassandrareader**<br />

<a name="jVb3v"></a>
## 二、支持的数据源版本
**Cassandra 3.0及以上**<br />

<a name="snQNv"></a>
## 三、参数说明

- **host**
  - 描述：数据库地址
  - 必选：是
  - 默认值：无



- **port**
  - 描述：端口
  - 必选：否
  - 默认值：9042



- **username**
  - 描述：用户名
  - 必选：否
  - 默认值：无



- **password**
  - 描述：密码
  - 必选：否
  - 默认值：无



- **useSSL**
  - 描述：数字证书
  - 必选：否
  - 默认值：false



- **column**
  - 描述：查询结果中被select出来的属性集合，为空则select *
  - 必选：否
  - 默认值：无



- **keyspace**
  - 描述：需要同步的表所在的keyspace
  - 必选：是
  - 默认值：无



- **table**
  - 描述：要查询的表
  - 必选：是
  - 默认值：无



- **where**
  - 描述：过滤条件where之后的表达式
  - 必选：否
  - 默认值：无



- **allowFiltering**
  - 描述：是否在服务端过滤数据
  - 必选：否
  - 默认值：false



- **connecttionsPerHost**
  - 描述：分配给每个host的连接数
  - 必选：否
  - 默认值：8



- **maxPendingPerConnection**
  - 描述：最多能建立的连接数
  - 必选：否
  - 默认值：128



- **consistancyLevel**
  - 描述：数据一致性级别。可选`ONE`、`QUORUM`、`LOCAL_QUORUM`、`EACH_QUORUM`、`ALL`、`ANY`、`TWO`、`THREE`、`LOCAL_ONE`
  - 必选：否
  - 默认值：无



<a name="oPPWY"></a>
## 四、配置示例
```json
{
  "job" : {
    "content" : [ {
      "reader": {
        "name": "cassandrareader",
        "parameter": {
          "host": "kudu3",
          "port": 9042,
          "username":"",
          "password":"",
          "useSSL":false,
          "column": [
            {
              "name": "rowkey",
              "type": "string"
            },
            {
              "name": "cf1:id",
              "type": "string"
            }
          ]
        }
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
        "channel": 1,
        "bytes": 0
      },
      "errorLimit": {
        "record": 1
      },
      "restore": {
        "maxRowNumForCheckpoint": 0,
        "isRestore": false,
        "restoreColumnName": "",
        "restoreColumnIndex": 0
      },
      "log" : {
        "isLogger": false,
        "level" : "debug",
        "path" : "",
        "pattern":""
      }
    }
  }
}
```


