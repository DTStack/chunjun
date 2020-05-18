# Kafka Writer

<a name="c6v6n"></a>
## 一、插件名称
kafka插件存在四个版本，根据kafka版本的不同，插件名称也略有不同。具体对应关系如下表所示：

| kafka版本 | 插件名称 |
| --- | --- |
| kafka 0.9 | kafka09writer |
| kafka 0.10 | kafka10writer |
| kafka 0.11 | kafka11writer |
| kafka 1.0及以上 | kafkawriter |



<a name="2lzA4"></a>
## 二、参数说明<br />

- **timezone**
  - 描述：时区
  - 必选：否
  - 默认值：无



- **topic**
  - 描述：topic
  - 必选：是
  - 默认值：无



- **encoding**
  - 描述：编码
  - 注意：该参数只对kafka09reader插件有效
  - 必选：否
  - 默认值：UTF-8



- **brokerList**
  - 描述：kafka broker地址列表
  - 注意：该参数只对kafka09writer插件有效
  - 必选：是
  - 默认值：无



- **producerSettings**
  - 描述：kafka连接配置，支持所有`org.apache.kafka.clients.producer.ProducerConfig`中定义的配置
  - 必选：是
  - 默认值：无



- **tableFields**
  - 描述：字段映射配置。从reader插件传递到writer插件的的数据只包含其value属性，配置该参数后可将其还原成键值对类型json字符串输出。
  - 注意：
    - 若配置该属性，则该配置中的字段个数必须不少于reader插件中读取的字段个数，否则该配置失效；
    - 映射关系按该配置中字段的先后顺序依次匹配；
  - 必选：否
  - 默认值：无



<a name="1LBc2"></a>
## 二、配置示例
<a name="CAiac"></a>
#### 1、kafka09
```json
{
  "job": {
    "content": [{
      "reader": {
        "name": "streamreader",
        "parameter": {
          "column": [
            {
              "name": "id",
              "type": "id"
            },
            {
              "name": "user_id",
              "type": "int"
            },
            {
              "name": "name",
              "type": "string"
            }
          ],
          "sliceRecordCount" : ["100"]
        }
      },
      "writer" : {
        "parameter": {
          "timezone": "UTC",
          "topic": "kafka09",
          "encoding": "UTF_8",
          "brokerList": "0.0.0.1:9092",
          "producerSettings": {
            "zookeeper.connect" : "0.0.0.1:2182",
            "bootstrap.servers" : "0.0.0.1:9092"
          },
          "tableFields": ["id","user_id","name"]
        },
        "name": "kafka09writer"
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
        "isStream" : false,
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
<a name="HphQR"></a>
#### 2、kafka10
```json
{
  "job": {
    "content": [{
      "reader": {
        "name": "streamreader",
        "parameter": {
          "column": [
            {
              "name": "id",
              "type": "id"
            },
            {
              "name": "user_id",
              "type": "int"
            },
            {
              "name": "name",
              "type": "string"
            }
          ],
          "sliceRecordCount" : ["100"]
        }
      },
      "writer" : {
        "parameter": {
          "timezone": "UTC",
          "topic": "kafka10",
          "producerSettings": {
            "zookeeper.connect" : "0.0.0.1:2182",
            "bootstrap.servers" : "0.0.0.1:9092"
          },
          "tableFields": ["id","user_id","name"]
        },
        "name": "kafka10writer"
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
        "isStream" : false,
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
<a name="rNfcr"></a>
#### 3、kafka11
```json
{
  "job": {
    "content": [{
      "reader": {
        "name": "streamreader",
        "parameter": {
          "column": [
            {
              "name": "id",
              "type": "id"
            },
            {
              "name": "user_id",
              "type": "int"
            },
            {
              "name": "name",
              "type": "string"
            }
          ],
          "sliceRecordCount" : ["100"]
        }
      },
      "writer" : {
        "parameter": {
          "timezone": "UTC",
          "topic": "kafka11",
          "producerSettings": {
            "zookeeper.connect" : "0.0.0.1:2182",
            "bootstrap.servers" : "0.0.0.1:9092"
          },
          "tableFields": ["id","user_id","name"]
        },
        "name": "kafka11writer"
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
        "isStream" : false,
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
<a name="NPYC9"></a>
#### 4、kafka
```json
{
  "job": {
    "content": [{
      "reader": {
        "name": "streamreader",
        "parameter": {
          "column": [
            {
              "name": "id",
              "type": "id"
            },
            {
              "name": "user_id",
              "type": "int"
            },
            {
              "name": "name",
              "type": "string"
            }
          ],
          "sliceRecordCount" : ["100"]
        }
      },
      "writer" : {
        "parameter": {
          "timezone": "UTC",
          "topic": "kafka",
          "producerSettings": {
            "zookeeper.connect" : "0.0.0.1:2182",
            "bootstrap.servers" : "0.0.0.1:9092"
          },
          "tableFields": ["id","user_id","name"]
        },
        "name": "kafkawriter"
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
        "isStream" : false,
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
<a name="vwDnZ"></a>
#### 5、MySQL->kafka
```json
{
  "job" : {
    "content" : [ {
      "reader": {
        "name": "mysqlreader",
        "parameter": {
          "column": ["id","user_id","name"],
          "username": "dtstack",
          "password": "abc123",
          "connection": [
            {
              "jdbcUrl": [
                "jdbc:mysql://kudu3:3306/tudou"
              ],
              "table": [
                "kudu"
              ]
            }
          ]
        }
      },
      "writer" : {
        "parameter" : {
          "tableFields" : ["id","user_id","name"],
          "producerSettings" : {
            "zookeeper.connect" : "kudu1:2182/kafka",
            "bootstrap.servers" : "kudu1:9092"
          },
          "topic" : "tudou"
        },
        "name" : "kafkawriter"
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
        "isStream" : false,
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


