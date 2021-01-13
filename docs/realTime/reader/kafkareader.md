# Kafka Reader

## 一、插件名称
kafka插件存在三个版本，根据kafka版本的不同，插件名称也略有不同。具体对应关系如下表所示：

| kafka版本 | 插件名称 |
| --- | --- |
| kafka 0.10 | kafka10reader |
| kafka 0.11 | kafka11reader |
| kafka 1.0及以后 | kafkareader |
注：从FlinkX1.11版本开始不再支持kafka 0.9



## 二、参数说明

- **topic**
   - 描述：要消费的topic，多个以,分割，当`mode`为`timestamp`、`specific-offsets`时不支持多topic
   - 必选：是
   - 字段类型：String
   - 默认值：无

<br />

- **mode**
   - 描述：kafka消费端启动模式，目前仅支持`kafkareader`插件
   - 可选值：
      - group-offsets：     从ZK / Kafka brokers中指定的消费组已经提交的offset开始消费
      - earliest-offset：    从最早的偏移量开始(如果可能)
      - latest-offset：      从最新的偏移量开始(如果可能)
      - timestamp：         从每个分区的指定的时间戳开始
      - specific-offsets： 从每个分区的指定的特定偏移量开始
   - 必选：否
   - 字段类型：String
   - 默认值：group-offsets

<br />

- **timestamp**
   - 描述：指定的kafka时间戳采集起点，目前仅支持`kafkareader`插件
   - 必选：当`mode`为`timestamp`时必选
   - 字段类型：Long
   - 默认值：无

<br />

- **offset**
   - 描述：消费的分区及对应的特定偏移量，目前仅支持`kafkareader`插件
   - 必选：当`mode`为`specific-offsets`时必选
   - 字段类型：String
   - 格式：partition:0,offset:42;partition:1,offset:300;partition:2,offset:300
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
   - 描述：编码解码器类型，支持 json、text
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
   - 注意：consumerSettings必须至少包含`bootstrap.servers`参数
   - 如：
```json
{
    "consumerSettings":{
        "bootstrap.servers":"host1:9092,host2:9092,host3:9092"
    }
}
```


## 三、配置示例
#### 1、kafka10
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
#### 2、kafka11
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
#### 3、kafka
```json
{
  "job" : {
    "content" : [ {
      "reader" : {
        "parameter" : {
          "topic" : "test",
          "mode": "timestamp",
          "timestamp": 1609812275000,
          "offset": "partition:0,offset:0;partition:1,offset:1;partition:2,offset:2",
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
#### 4、kafka->Hive
```json
{
  "job": {
    "content": [
      {
        "reader" : {
          "parameter" : {
            "topic" : "test",
            "mode": "timestamp",
            "timestamp": 1609812275000,
            "codec": "text",
            "consumerSettings" : {
              "bootstrap.servers" : "ip1:9092,ip2:9092,ip3:9092"
            }
          },
          "name" : "kafkareader"
        },
        "writer": {
          "parameter" : {
            "jdbcUrl" : "jdbc:hive2://ip:10000/test",
            "fileType" : "parquet",
            "writeMode" : "overwrite",
            "compress" : "",
            "charsetName" : "UTF-8",
            "maxFileSize" : 1073741824,
            "tablesColumn" : "{\"message\":[{\"part\":false,\"comment\":\"\",\"type\":\"string\",\"key\":\"message\"}]}",
            "partition" : "pt",
            "partitionType" : "DAY",
            "defaultFS" : "hdfs://ns",
            "hadoopConfig": {
              "dfs.ha.namenodes.ns": "nn1,nn2",
              "dfs.namenode.rpc-address.ns.nn2": "ip1:9000",
              "dfs.client.failover.proxy.provider.ns": "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider",
              "dfs.namenode.rpc-address.ns.nn1": "ip2:9000",
              "dfs.nameservices": "ns"
            }
          },
          "name" : "hivewriter"
        }
      }
    ],
    "setting": {
      "restore": {
        "isRestore": true,
        "isStream": true
      },
      "speed": {
        "readerChannel": 3,
        "writerChannel": 1
      }
    }
  }
}
```