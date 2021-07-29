# Kafka Writer

## 一、插件名称
kafka插件存在四个版本，根据kafka版本的不同，插件名称也略有不同。具体对应关系如下表所示：

| kafka版本 | 插件名称 |
| --- | --- |
| kafka 0.9 | kafka09writer |
| kafka 0.10 | kafka10writer |
| kafka 0.11 | kafka11writer |
| kafka 1.0及以后 | kafkawriter |



## 二、参数说明

- **topic**
   - 描述：消息发送至kafka的topic名称，不支持多个topic
   - 必选：是
   - 字段类型：String
   - 默认值：无

<br />

- **timezone**
   - 描述：时区
   - 必选：否
   - 字段类型：String
   - 默认值：无

<br />

- **encoding**
   - 描述：编码
   - 注意：该参数只对kafka09reader插件有效
   - 必选：否
   - 字段类型：String
   - 默认值：UTF-8

<br />

- **brokerList**
   - 描述：kafka broker地址列表
   - 注意：该参数只对kafka09writer插件有效
   - 必选：kafka09writer必选，其它kafka writer插件不用填
   - 字段类型：String
   - 默认值：无

<br />

- **producerSettings**
   - 描述：kafka连接配置，支持所有`org.apache.kafka.clients.producer.ProducerConfig`中定义的配置
   - 必选：对于非kafka09 writer插件，该参数必填，且producerSettings中至少包含`bootstrap.servers`参数
   - 字段类型：Map
   - 默认值：无

<br />

- **tableFields**
   - 描述：字段映射配置。从reader插件传递到writer插件的的数据只包含其value属性，配置该参数后可将其还原成键值对类型json字符串输出。
   - 注意：
      - 若配置该属性，则该配置中的字段个数必须不少于reader插件中读取的字段个数，否则该配置失效；
      - 映射关系按该配置中字段的先后顺序依次匹配；
   - 必选：否
   - 字段类型：String[]
   - 默认值：无



## 三、配置示例
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
          "encoding": "UTF-8",
          "brokerList": "0.0.0.1:9092",
          "tableFields": ["id","user_id","name"]
        },
        "name": "kafka09writer"
      }
    } ],
    "setting": {
      "restore" : {
        "isStream" : true
      },
      "speed" : {
        "channel" : 1
      }
    }
  }
}
```
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
            "bootstrap.servers" : "0.0.0.1:9092"
          },
          "tableFields": ["id","user_id","name"]
        },
        "name": "kafka10writer"
      }
    } ],
    "setting": {
      "restore" : {
        "isStream" : true
      },
      "speed" : {
        "channel" : 1
      }
    }
  }
}
```
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
      "restore" : {
        "isStream" : true
      },
      "speed" : {
        "channel" : 1
      }
    }
  }
}
```
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
            "bootstrap.servers" : "0.0.0.1:9092"
          },
          "tableFields": ["id","user_id","name"]
        },
        "name": "kafkawriter"
      }
    } ],
    "setting": {
      "restore" : {
        "isStream" : true
      },
      "speed" : {
        "channel" : 1
      }
    }
  }
}
```
#### 5、MySQL->kafka
```json
{
  "job" : {
    "content" : [ {
      "reader": {
        "name": "mysqlreader",
        "parameter": {
          "column": ["id","user_id","name"],
          "username": "username",
          "password": "password",
          "connection": [
            {
              "jdbcUrl": [
                "jdbc:mysql://0.0.0.1:3306/test"
              ],
              "table": [
                "test"
              ]
            }
          ]
        }
      },
      "writer" : {
        "parameter" : {
          "tableFields" : ["id","user_id","name"],
          "producerSettings" : {
            "bootstrap.servers" : "0.0.0.1:9092"
          },
          "topic" : "kafka"
        },
        "name" : "kafkawriter"
      }
    } ],
   "setting": {
      "restore" : {
        "isStream" : true
      },
      "speed" : {
        "channel" : 1
      }
    }
  }
}
```


