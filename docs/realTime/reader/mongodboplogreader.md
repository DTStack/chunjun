# MongoDB Oplog Reader

<a name="QMYmg"></a>
## 一、插件名称
名称：**mongodboplogreader**<br />

<a name="UaR1L"></a>
## 二、数据源版本
**MongoDB 4.0及以上**<br />

<a name="oVYPr"></a>
## 三、数据源配置
[MongoDB 4.0副本集搭建](https://dtstack.yuque.com/rd-center/udi643/gufhya)<br />

<a name="2Dcvs"></a>
## 四、参数说明

- **hostPorts**
  - 描述：Mongodb集群地址
  - 必选：是
  - 默认值：无



- **username**
  - 描述： 用户名
  - 必选：是
  - 默认值：无



- **password**
  - 描述： 密码
  - 必选：是
  - 默认值：无



- **authenticationMechanism**
  - 描述： 认证机制，可选：GSSAPI、PLAIN、MONGODB-X509、MONGODB-CR、SCRAM-SHA-1、SCRAM-SHA-256
  - 必选：否
  - 默认值：无



- **clusterMode**
  - 描述： 集群模式，可选：REPLICA_SET、MASTER_SLAVE
  - 必选：是
  - 默认值：无



- **monitorDatabases**
  - 描述： 要监听的库
  - 必选：否
  - 默认值：无



- **monitorCollections**
  - 描述：要监听的集合
  - 必选：否
  - 默认值：无



- **operateType**
  - 描述：要监听的操作类型，可选：insert、update、delete
  - 必选：否
  - 默认值：无



- **excludeDocId**
  - 描述：是否排除_id字段
  - 必选：否
  - 默认值：false



- **pavingData**
  - 描述：是否将解析出的json数据拍平
  - 示例：假设解析的表为tb1,数据库为test，对tb1中的id字段做update操作，id原来的值为1，更新后为2，则pavingData为true时数据格式为：
```json
{
    "type":"update",
    "schema":"test",
    "table":"tb1",
    "ts":1231232,
    "ingestion":123213,
    "before_id":1,
    "after_id":2
}
```

  - <br />pavingData为false时：
```json
{
    "message":{
         "type":"update",
         "schema":"test",
         "table":"tb1",
         "ts":1231232,
         "ingestion":123213,
         "before_id":{
             "id":1
         },
         "after_id":{
             "id":2
         }
    }
}
```
其中”ts“是数据变更时间，ingestion是插件解析这条数据的纳秒时间

  - 必选：否
  - 默认值：false



<a name="3Cis7"></a>
## 五、使用示例
<a name="lFoBe"></a>
#### 采集test库下的所有集合
```json
{
  "job": {
    "content": [
      {
        "reader": {
          "name": "mongodboplogreader",
          "parameter": {
            "hostPorts": "127.0.0.1:30001,127.0.0.1:30002,127.0.0.1:30003",
            "username": "root",
            "password": "123456",
            "database": "admin",
            "clusterMode": "REPLICA_SET",
            "authenticationMechanism": "SCRAM-SHA-256",
            "monitorDatabases": ["test"],
            "monitorCollections":[],
            "operateType":["insert","update","delete"],
            "pavingData":true,
            "excludeDocId": false
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
        "bytes": 1048576
      },
      "errorLimit": {
        "record": 100
      },
      "restore" : {
        "isRestore" : true,
        "isStream" : true
      }
    }
  }
}
```


