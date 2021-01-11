# Kafka Reader

## 一、插件名称
kafka插件存在四个版本，根据kafka版本的不同，插件名称也略有不同。具体对应关系如下表所示：

| kafka版本 | 插件名称 |
| --- | --- |
| kafka 0.9 | kafka09reader |
| kafka 0.10 | kafka10reader |
| kafka 0.11 | kafka11reader |
| kafka 1.0及以后 | kafkareader |



## 二、参数说明

- **topic**
   - 描述：要消费的topic，多个以,分割
   - 必选：是
   - 字段类型：String
   - 默认值：无

<br />


- **groupId**
   - 描述：kafka消费组Id
   - 必选：否
   - 字段类型：String
   - 默认值：default

<br />

- **encoding**
   - 描述：字符编码
   - 必选：否
   - 字段类型：String
   - 默认值：UTF-8

<br />

- **codec**
   - 描述：编码解码器类型，支持 json、plain
      - text：
		将kafka获取到的消息字符串存储到一个key为message的map中，如：kafka中的消息为：{"key":"key","message":"value"}，
		则发送至下游的数据格式为：

		```json
		[
			{
				"message":"{\"key\": \"key\", \"value\": \"value\"}"
			}
		]
		```

 	- json：将kafka获取到的消息字符串按照json格式进行解析
    	 - 若该字符串为json格式
     		- 当其中含有message字段时，发送至下游的数据格式为：
				```json
				[
					{
						"key":"key",
						"message":"value"
					}
				]
				```

		- 当其中不包含message字段时，增加一个key为message，value为原始消息字符串的键值对，发送至下游的数据格式为：
			```json
			[
				{
					"key":"key",
					"value":"value",
					"message":"{\"key\": \"key\", \"value\": \"value\"}"
				}
			]
			```
         - 若改字符串不为json格式，则按照text类型进行处理
   - 必选：否
   - 字段类型：String
   - 默认值：text

<br />

- **blankIgnore**
   - 描述：是否忽略空值消息
   - 必选：否
   - 字段类型：Boolean
   - 默认值：false

<br />

- **consumerSettings**
   - 描述：kafka连接配置，支持所有`kafka.consumer.ConsumerConfig.ConsumerConfig`中定义的配置
   - 必选：是
   - 字段类型：Map
   - 默认值：无
   - 注意：
      - kafka09 reader插件: consumerSettings必须至少包含`zookeeper.connect`参数
      - kafka09 reader以外的插件：consumerSettings必须至少包含`bootstrap.servers`参数
   - 如：
```json
{
    "consumerSettings":{
        "bootstrap.servers":"host1:9092,host2:9092,host3:9092"
    }
}
```


## 三、配置示例
#### 1、kafka09
```json
{
  "job" : {
    "content" : [ {
      "reader" : {
        "parameter" : {
          "topic" : "kafka09",
          "groupId" : "default",
          "codec" : "text",
          "encoding": "UTF-8",
          "blankIgnore": false,
          "consumerSettings" : {
            "zookeeper.connect" : "localhost:2181/kafka09"
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
    "setting" : {
      "restore" : {
        "isRestore" : false,
        "isStream" : true
      },
      "speed" : {
        "channel" : 1
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
    "content": [
      {
        "reader": {
          "parameter": {
            "topic": "kafka10",
            "groupId": "default",
            "codec": "text",
            "encoding": "UTF-8",
            "blankIgnore": false,
            "consumerSettings": {
              "bootstrap.servers": "localhost:9092"
            }
          },
          "name": "kafka10reader"
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
        "isRestore": false,
        "isStream": true
      },
      "speed": {
        "channel": 1
      }
    }
  }
}
```
#### 3、kafka11
```json
{
  "job" : {
    "content" : [ {
      "reader" : {
        "parameter" : {
          "topic" : "kafka11",
          "groupId": "default",
          "codec": "text",
          "encoding": "UTF-8",
          "blankIgnore": false,
          "consumerSettings": {
            "bootstrap.servers": "localhost:9092"
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
    "setting" : {
      "restore" : {
        "isRestore" : false,
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
  "job" : {
    "content" : [ {
      "reader" : {
        "parameter" : {
          "topic" : "test",
          "codec": "text",
          "blankIgnore": false,
          "consumerSettings" : {
            "bootstrap.servers" : "ip1:9092,ip2:9092,ip3:9092"
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
      "restore" : {
        "isRestore" : false,
        "isStream" : true
      },
      "speed": {
        "readerChannel": 3,
        "writerChannel": 1
      }
    }
  }
}
```
#### 5、kafka->Hive
```json
{
  "job" : {
    "content" : [ {
      "reader" : {
        "parameter" : {
          "codec": "json",
          "groupId" : "default",
          "topic" : "test",
          "consumerSettings" : {
            "bootstrap.servers" : "ip1:9092,ip2:9092,ip3:9092"
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
          "username": "username",
          "password": "password",
          "batchSize": 1,
          "connection": [
            {
              "jdbcUrl": "jdbc:mysql://localhost:3306/tudou?useSSL=false",
              "table": [
                "test"
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