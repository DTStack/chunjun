# Kafka Reader

<a name="c6v6n"></a>
## 一、插件名称
kafka插件存在四个版本，根据kafka版本的不同，插件名称也略有不同。具体对应关系如下表所示：

| kafka版本 | 插件名称 |
| --- | --- |
| kafka 0.9 | kafka09reader |
| kafka 0.10 | kafka10reader |
| kafka 0.11 | kafka11reader |
| kafka 1.0及以上 | kafkareader |

<br />
<a name="8Xm5p"></a>
二、参数说明<br />

- **topic**
  - 描述：要消费的topic
  - 必选：是
  - 默认值：无



- **groupId**
  - 描述：kafka消费组Id
  - 注意：该参数对kafka09reader插件无效
  - 必选：是
  - 默认值：无



- **encoding**
  - 描述：字符编码
  - 注意：该参数只对kafka09reader插件有效
  - 必选：否
  - 默认值：UTF-8



- **codec**
  - 描述：编码解码器类型，支持 json、plain
    - plain：将kafka获取到的消息字符串存储到一个key为message的map中，如：`{"message":"{\"key\": \"key\", \"message\": \"value\"}"}`
    - json：将kafka获取到的消息字符串按照json格式进行解析
      - 若该字符串为json格式
        - 当其中含有message字段时，原样输出，如：`{"key": "key", "message": "value"}`
        - 当其中不包含message字段时，增加一个key为message，value为原始消息字符串的键值对，如：`{"key": "key", "value": "value", "message": "{\"key\": \"key\", \"value\": \"value\"}"}`
      - 若改字符串不为json格式，则按照plain类型进行处理
  - 必选：否
  - 默认值：plain



- **blankIgnore**
  - 描述：是否忽略空值消息
  - 必选：否
  - 默认值：false



- **consumerSettings**
  - 描述：kafka连接配置，支持所有`kafka.consumer.ConsumerConfig.ConsumerConfig`中定义的配置
  - 必选：是
  - 默认值：无



- **column**
  - 描述：需要读取的字段。
  - 格式：
```json
"column": [{
    "name": "col",
    "type": "datetime",
    "format": "yyyy-MM-dd hh:mm:ss",
    "value": "value"
}]
```

  - 属性说明:
    - name：字段名称
    - type：字段类型，可以和数据库里的字段类型不一样，程序会做一次类型转换
    - format：如果字段是时间字符串，可以指定时间的格式，将字段类型转为日期格式返回
    - value：如果数据库里不存在指定的字段，则会把value的值作为常量列返回，如果指定的字段存在，当指定字段的值为null时，会以此value值作为默认值返回
  - 必选：是
  - 默认值：无


<a name="ftKiS"></a>
## 三、配置示例
<a name="VaSXt"></a>
#### 1、kafka09
```json
{
  "job": {
    "content": [{
      "reader" : {
        "parameter" : {
          "topic" : "kafka09",
          "codec": "plain",
          "encoding": "UTF-8",
          "consumerSettings" : {
            "zookeeper.connect" : "0.0.0.1:2182/kafka09",
            "group.id" : "default",
            "auto.commit.interval.ms" : "1000",
            "auto.offset.reset" : "smallest"
          }
        },
        "name" : "kafka09reader"
      },
      "writer" : {
        "parameter" : {
          "print" : true
        },
        "name" : "streamwriter"
      }
    } ],
    "setting": {
      "speed": {
        "channel": 1,
        "bytes": 0
      },
      "errorLimit": {
        "record": 100
      },
      "restore": {
        "maxRowNumForCheckpoint": 0,
        "isRestore": false,
        "isStream" : true,
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
<a name="BbqQ7"></a>
#### 2、kafka10
```json
{
  "job": {
    "content": [{
      "reader" : {
        "parameter" : {
          "topic" : "kafka10",
          "groupId" : "default",
          "codec": "plain",
          "blankIgnore": false,
          "consumerSettings" : {
            "zookeeper.connect" : "0.0.0.1:2182/kafka",
            "bootstrap.servers" : "0.0.0.1:9092",
            "auto.commit.interval.ms" : "1000",
            "auto.offset.reset" : "latest"
          }
        },
        "name" : "kafka10reader"
      },
      "writer" : {
        "parameter" : {
          "print" : true
        },
        "name" : "streamwriter"
      }
    } ],
    "setting": {
      "speed": {
        "channel": 1,
        "bytes": 0
      },
      "errorLimit": {
        "record": 100
      },
      "restore": {
        "maxRowNumForCheckpoint": 0,
        "isRestore": false,
        "isStream" : true,
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
<a name="Jl0dk"></a>
#### 3、kafka11
```json
{
  "job": {
    "content": [{
      "reader" : {
        "parameter" : {
          "topic" : "kafka11",
          "groupId" : "default",
          "codec": "plain",
          "blankIgnore": false,
          "consumerSettings" : {
            "zookeeper.connect" : "0.0.0.1:2182/kafka",
            "bootstrap.servers" : "0.0.0.1:9092",
            "auto.commit.interval.ms" : "1000",
            "auto.offset.reset" : "latest"
          }
        },
        "name" : "kafka11reader"
      },
      "writer" : {
        "parameter" : {
          "print" : true
        },
        "name" : "streamwriter"
      }
    } ],
    "setting": {
      "speed": {
        "channel": 1,
        "bytes": 0
      },
      "errorLimit": {
        "record": 100
      },
      "restore": {
        "maxRowNumForCheckpoint": 0,
        "isRestore": false,
        "isStream" : true,
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
<a name="SvAwr"></a>
#### 4、kafka
```json
{
  "job": {
    "content": [{
      "reader" : {
        "parameter" : {
          "topic" : "kafka",
          "groupId" : "default",
          "codec": "plain",
          "blankIgnore": false,
          "consumerSettings" : {
            "zookeeper.connect" : "0.0.0.1:2182/kafka",
            "bootstrap.servers" : "0.0.0.1:9092",
            "auto.commit.interval.ms" : "1000",
            "auto.offset.reset" : "latest"
          }
        },
        "name" : "kafkareader"
      },
      "writer" : {
        "parameter" : {
          "print" : true
        },
        "name" : "streamwriter"
      }
    } ],
    "setting": {
      "speed": {
        "channel": 1,
        "bytes": 0
      },
      "errorLimit": {
        "record": 100
      },
      "restore": {
        "maxRowNumForCheckpoint": 0,
        "isRestore": false,
        "isStream" : true,
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
<a name="4oSeP"></a>
#### 5、kafka->MySQL
```json
{
  "job" : {
    "content" : [ {
      "reader" : {
        "parameter" : {
          "codec": "json",
          "groupId" : "default",
          "topic" : "tudou",
          "consumerSettings" : {
            "zookeeper.connect" : "kudu1:2181/kafka",
            "bootstrap.servers" : "kudu1:9092",
            "auto.commit.interval.ms" : "1000",
            "auto.offset.reset" : "earliest"
          },
          "column": [
            {
              "name": "id",
              "type": "BIGINT"
            },
            {
              "name": "user_id",
              "type": "BIGINT"
            },
            {
              "name": "name",
              "type": "VARCHAR"
            }
          ]
        },
        "name" : "kafkareader"
      },
      "writer": {
        "name": "mysqlwriter",
        "parameter": {
          "username": "dtstack",
          "password": "abc123",
          "batchSize": 1,
          "connection": [
            {
              "jdbcUrl": "jdbc:mysql://kudu3:3306/tudou?useSSL=false",
              "table": [
                "kudu"
              ]
            }
          ],
          "session": [],
          "preSql": [],
          "postSql": [],
          "writeMode": "insert",
          "column": [
            {
              "name": "id",
              "type": "BIGINT"
            },
            {
              "name": "user_id",
              "type": "BIGINT"
            },
            {
              "name": "name",
              "type": "VARCHAR"
            }
          ]
        }
      }
    } ],
    "setting": {
      "speed": {
        "channel": 1,
        "bytes": 0
      },
      "errorLimit": {
        "record": 100
      },
      "restore": {
        "maxRowNumForCheckpoint": 0,
        "isRestore": false,
        "isStream" : true,
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
<br />
