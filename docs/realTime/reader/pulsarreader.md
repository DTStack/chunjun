# Pulsar Reader

<!-- TOC -->

- [Pulsar Reader](#Pulsar-reader)
    - [一、插件名称](#一插件名称)
    - [二、参数说明](#二参数说明)
    - [三、配置示例](#三配置示例)
        - [1、pulsar-mysql](#1pulsar-mysql)
        - [2、Pulsar->Hive](#2Pulsar->Hive)
        - [3、Pulsar->Pulsar](#3Pulsar->Pulsar)

<!-- /TOC -->

<br/>

## 一、插件名称
pulsarreader插件目前基于2.5.0版本，以Consumer客户端的方式支持InitialPosition，暂不支持Pulsar原生Reader客户端的指定MessageId的方式。

<br/>

## 二、参数说明

- **topic**
   - 描述：要消费的topic，多个以,分割。
   - 必选：是
   - 字段类型：String
   - 默认值：无

<br />

- **token**
   - 描述：访问Pulsar集群需要的token，若无token验证可为空。
   - 必选：否
   - 字段类型：String
   - 默认值：无

<br />

- **initialPosition**
   - 描述：pulsar消费端启动位置，参考org.apache.pulsar.client.api.SubscriptionInitialPosition。
   - 可选值：
      - Latest：     从topic最新的位置开始消费
      - Earliest：    从最早的位置开始(如果可能)。当消息被consumer.acknowledge之后是消费不到的，
   - 必选：否
   - 字段类型：String
   - 默认值：Latest

<br />

- **codec**
   - 描述：编码解码器类型，支持 json、text、flat
      - text：
		将pulsar获取到的消息字符串存储到一个key为message的map中，如：pulsar中的消息为：{"key":"key","message":"value"}，
		则发送至下游的数据格式为：

		```json
		[
			{
				"message":"{\"key\": \"key\", \"message\": \"value\"}"
			}
		]
		```

 	- json：将pulsar获取到的消息字符串按照json格式进行解析
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
      - flat： 将pulsar获取到的消息字符串按照csv分隔符的格式解析。如：列名为id,name,age，数据为：1,乔丹,23 。
   - 必选：否
   - 字段类型：String
   - 默认值：text

<br />

- **fieldDelimiter**
   - 描述：字段分隔符，当codec=flat模式时的分隔符，默认为英文逗号","
   - 必选：否
   - 字段类型：String
   - 默认值：,

<br />

- **blankIgnore**
   - 描述：是否忽略空值消息
   - 必选：否
   - 字段类型：Boolean
   - 默认值：false

<br />

- **pulsarServiceUrl**
  - 描述：pulsar服务地址
  - 必选：是
  - 字段类型：String
  - 默认值："pulsar://localhost:6650"

<br />

- **consumerSettings**
   - 描述：pulsar原生配置，参考：[Pulsar-Configure consumer](http://pulsar.apache.org/docs/en/client-libraries-java/#configure-consumer)
   - 必选：否
   - 字段类型：Map
   - 默认值：无
   - 如：
```json
{
    "consumerSettings":{
       "consumerName":"flinkx"
    }
}
```

<br/>

<a name="ftKiS"></a>
## 三、配置示例
### 1、pulsar-mysql
```json
{
  "job" : {
    "content" : [ {
      "reader" : {
        "parameter": {
          "topic": "flinkx",
          "timeout": 10000,
          "blankIgnore": false,
          "column": ["id","name","age"],
          "pulsarServiceUrl": "pulsar://localhost:6650",
          "consumerSettings": {
            "consumerName":"flinkx"
          }
        },
        "name": "pulsarreader"
      },
      "writer" : {
        "parameter": {
          "username": "mysql",
          "password": "password",
          "connection": [
            {
              "jdbcUrl": "jdbc:mysql://localhost:3306/flinkx?serverTimezone=UTC&characterEncoding=UTF-8",
              "table": ["table_01"]
            }
          ],
          "writeMode": "insert",
          "column": ["id","name","age"],
          "batchSize": 1000
        },
        "name": "mysqlwriter"
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
### 2、Pulsar->Hive
```json
{
  "job": {
    "content": [
      {
        "reader" : {
          "parameter": {
            "topic": "flinkx",
            "timeout": 10000,
            "blankIgnore": false,
            "column": ["id","name","age"],
            "pulsarServiceUrl": "pulsar://localhost:6650",
            "consumerSettings": {
              "consumerName":"flinkx"
            }
          },
          "name": "pulsarreader"
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

### 3、Pulsar->Pulsar
```json
{
  "job": {
    "content": [
      {
        "reader" : {
          "parameter": {
            "topic": "persistent://public/default/flinkx_level1",
            "timeout": 10000,
            "blankIgnore": false,
            "codec": "json",
            "initialPosition": "Earliest",
            "pulsarServiceUrl": "pulsar://localhost:6650",
            "consumerSettings": {
              "consumerName":"flinkx"
            }
          },
          "name": "pulsarreader"
        },
        "writer": {
          "parameter" : {
            "topic": "persistent://public/default/flinkx_level2",
            "timeout": 10000,
            "blankIgnore": false,
            "tableFields": [],
            "pulsarServiceUrl": "pulsar://localhost:6650",
            "producerSettings": {
              "producerName": "flinkx"
            }
          },
          "name" : "pulsarwriter"
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