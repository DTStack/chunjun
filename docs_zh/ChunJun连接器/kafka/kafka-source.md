

## 一、介绍

Kafka Source

## 二、支持版本

kafka主流版本

## 三、插件名称
| Sync | kafkasource、kafkareader |
| --- | --- |
| SQL | kafka-x |
| SQL(upsert) | upsert-kafka-x |


## 四、参数说明

### 1、Sync

-  **topic**
    - 描述：要消费的topic，多个以,分割，当`mode`为`timestamp`、`specific-offsets`时不支持多topic
    - 必选：是
    - 字段类型：String
    - 默认值：无
-  **mode**
    - 描述：kafka消费端启动模式，目前仅支持`kafkareader`插件
    - 可选值：
        - group-offsets：     从ZK / Kafka brokers中指定的消费组已经提交的offset开始消费
        - earliest-offset：    从最早的偏移量开始(如果可能)
        - latest-offset：      从最新的偏移量开始(如果可能)
        - timestamp：         从每个分区的指定的时间戳开始
        - specific-offsets： 从每个分区的指定的特定偏移量开始
    - 必选：否
    - 字段类型：String
    - 默认值：group-offsets
-  **timestamp**
    - 描述：指定的kafka时间戳采集起点，目前仅支持`kafkareader`插件
    - 必选：当`mode`为`timestamp`时必选
    - 字段类型：Long
    - 默认值：无
-  **offset**
    - 描述：消费的分区及对应的特定偏移量，目前仅支持`kafkareader`插件
    - 必选：当`mode`为`specific-offsets`时必选
    - 字段类型：String
    - 格式：partition:0,offset:42;partition:1,offset:300;partition:2,offset:300
    - 默认值：无
-  **groupId**
    - 描述：kafka消费组Id
    - 必选：否
    - 字段类型：String
    - 默认值：default
-  **encoding**
    - 描述：字符编码
    - 必选：否
    - 字段类型：String
    - 默认值：UTF-8
-  **codec**
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
-  **consumerSettings**
    - 描述：kafka连接配置，支持所有`kafka.consumer.ConsumerConfig.ConsumerConfig`中定义的配置
    - 必选：是
    - 字段类型：Map
    - 默认值：无
    - 如：
```json
{
    "consumerSettings":{
        "bootstrap.servers":"host1:9092,host2:9092,host3:9092"
    }
}
```

-  **column**
    - 描述：kafka向MySQL写数据时，对应MySQL表中的字段名
    - 必选：否
    - 字段类型：List
    - 默认值：无
    - 注意：需指定字段的具体信息，属性说明：
        - name：字段名称
        - type：字段类型，可以和数据库里的字段类型不一样，程序会做一次类型转换
        - format：如果字段是时间字符串，可以指定时间的格式，将字段类型转为日期格式返回
        - 如：
```json
{
   "column": [
      {
         "name": "col",
         "type": "datetime",
         "format": "yyyy-MM-dd hh:mm:ss"
      }
   ]
}
```


### 2、SQL

具体可以参考：[kafka-connector](https://ci.apache.org/projects/flink/flink-docs-release-1.12/dev/table/connectors/kafka.html)

-  **connector**
    - 描述：kafka-x
    - 必选：是
    - 字段类型：String
    - 默认值：无
-  **topic**
    - 描述：当表用作源时要从中读取数据的主题名称。它还通过用分号分隔主题来支持源的主题列表，如'topic-1;topic-2'. 请注意，只能为源指定“topic-pattern”和“topic”之一。当表用作接收器时，主题名称是要写入数据的主题。接收器不支持注意主题列表。
    - 必选：是
    - 字段类型：String
    - 默认值：无
-  **topic-pattern**
    - 描述：要从中读取的主题名称模式的正则表达式。当作业开始运行时，消费者将订阅名称与指定正则表达式匹配的所有主题。请注意，只能为源指定“topic-pattern”和“topic”之一。
    - 必选：否
    - 字段类型：String
    - 默认值：无
-  **properties.bootstrap.servers**
    - 描述：逗号分隔的 Kafka 代理列表。
    - 必选：是
    - 字段类型：String
    - 默认值：无
-  **properties.group.id**
    - 描述：Kafka source的消费组id，Kafka sink可选。
    - 必选：required by source
    - 字段类型：String
    - 默认值：无
-  **properties.***
    - 描述：这可以设置和传递任意 Kafka 配置。后缀名称必须与[Kafka 配置文档中](https://kafka.apache.org/documentation/#configuration)定义的配置键匹配。Flink 将删除“属性”。键前缀并将转换后的键和值传递给底层 KafkaClient。例如，您可以通过 禁用自动主题创建'properties.allow.auto.create.topics' = 'false'。但是有一些配置是不支持设置的，因为 Flink 会覆盖它们。
    - 必选：否
    - 字段类型：String
    - 默认值：无
-  **format**
    - 描述：用于反序列化和序列化 Kafka 消息的值部分的格式。有关更多详细信息和更多[格式](https://ci.apache.org/projects/flink/flink-docs-release-1.12/zh/dev/table/connectors/formats/)选项，请参阅格式页面。注意：此选项或'value.format'选项都是必需的。
    - 必选：是
    - 字段类型：String
    - 默认值：无
-  **key.format**
    - 描述：用用于反序列化和序列化 Kafka 消息关键部分的格式。有关更多详细信息和更多[格式](https://ci.apache.org/projects/flink/flink-docs-release-1.12/zh/dev/table/connectors/formats/)选项，请参阅格式页面。注意：如果定义了密钥格式，则该'key.fields' 选项也是必需的。否则 Kafka 记录将有一个空键。
    - 必选：否
    - 字段类型：String
    - 默认值：无
-  **key.fields**
    - 描述：定义表架构中物理列的显式列表，用于配置键格式的数据类型。默认情况下，此列表为空，因此未定义键。该列表应如下所示'field1;field2'。
    - 必选：否
    - 字段类型：List
    - 默认值：无
-  **key.fields-prefix**
    - 描述：为键格式的所有字段定义自定义前缀，以避免与值格式的字段发生名称冲突。默认情况下，前缀为空。如果定义了自定义前缀，则表架构 和'key.fields'都将使用前缀名称。在构造密钥格式的数据类型时，将删除前缀，并在密钥格式中使用非前缀名称。请注意，此选项要求'value.fields-include' 必须设置为'EXCEPT_KEY'。
    - 必选：否
    - 字段类型：String
    - 默认值：无
-  **value.format**
    - 描述：用于反序列化和序列化 Kafka 消息的值部分的格式。有关更多详细信息和更多格式选项，请参阅格式页面。注意：此选项或'format'[选项](https://ci.apache.org/projects/flink/flink-docs-release-1.12/zh/dev/table/connectors/formats/)都是必需的。
    - 必选：是
    - 字段类型：String
    - 默认值：无
-  **value.fields-include**
    - 描述：定义如何处理值格式的数据类型中的键列的策略。默认情况下，'ALL'表模式的物理列将包含在值格式中，这意味着键列出现在键和值格式的数据类型中
    - 必选：否
    - 字段类型：枚举
        - 可选的值：[ALL, EXCEPT_KEY]
    - 默认值：ALL
-  **scan.startup.mode**
    - 描述：kafka消费的启动模式，有效值为'earliest-offset'，'latest-offset'，'group-offsets'，'timestamp'和'specific-offsets'。有关更多详细信息，请参阅以下[开始阅读位置](https://ci.apache.org/projects/flink/flink-docs-release-1.12/zh/dev/table/connectors/kafka.html#start-reading-position)。upsert模式此参数不生效，写死从earliest-offset处消费
    - 必选：否
    - 字段类型：String
    - 默认值：group-offsets
-  **scan.startup.specific-offsets**
    - 描述：在'specific-offsets'启动模式下为每个分区指定偏移量，例如'partition:0,offset:42;partition:1,offset:300'.
    - 必选：否
    - 字段类型：String
    - 默认值：无
-  **scan.startup.timestamp-millis**
    - 描述：从'timestamp'启动模式下使用的指定纪元时间戳（毫秒）开始。
    - 必选：否
    - 字段类型：Long
    - 默认值：无
-  **scan.topic-partition-discovery.interval**
    - 描述：消费者定期发现动态创建的 Kafka 主题和分区的时间间隔。
    - 必选：否
    - 字段类型：Duration
    - 默认值：无
-  **sink.partitioner**
    - 描述： 从 Flink 的分区到 Kafka 的分区的输出分区。有效值为
    - default: 使用 kafka 默认分区器对记录进行分区。
    - fixed：每个 Flink 分区最终最多包含一个 Kafka 分区。
    - round-robin：一个 Flink 分区被分发到 Kafka 分区粘性循环。它仅在未指定记录的键时有效。
    - 自定义FlinkKafkaPartitioner子类：例如'org.mycompany.MyPartitioner'.
    - 有关更多详细信息，请参阅以下[接收器分区](https://ci.apache.org/projects/flink/flink-docs-release-1.12/zh/dev/table/connectors/kafka.html#sink-partitioning)。
    - 必选：否
    - 字段类型：String
    - 默认值：default
-  **scan.parallelism**
    - 描述：定义 Kafka sink 操作符的并行性。默认情况下，并行度由框架使用与上游链式运算符相同的并行度确定。
    - 必选：否
    - 字段类型：Integer
    - 默认值：无

## 五、数据类型
| 支持 | BOOLEAN、TINYINT、SMALLINT、INT、BIGINT、FLOAT、DOUBLE、DECIMAL、STRING、VARCHAR、CHAR、TIMESTAMP、DATE、BINARY、ARRAY、MAP、STRUCT、LIST、ROW |
| --- | --- |
| 暂不支持 | 其他 |


## 六、脚本示例

见项目内`chunjun-examples`文件夹。
