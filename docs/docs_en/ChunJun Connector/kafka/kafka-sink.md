

## 一、介绍

kafka sink

## 二、支持版本

kafka主流版本

## 三、插件名称
| Sync | kafkasink、kafkawriter |
| --- | --- |
| SQL | kafka-x |
| SQL(upsert) | upsert-kafka-x |


## 四、参数说明

### 1、Sync

-  **topic**
    - 描述：消息发送至kafka的topic名称，不支持多个topic
    - 必选：是
    - 字段类型：String
    - 默认值：无
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


- **tableFields**
    - 描述：字段映射配置。从reader插件传递到writer插件的的数据只包含其value属性，配置该参数后可将其还原成键值对类型json字符串输出。
    - 注意：
        - 若配置该属性，则该配置中的字段个数必须不少于reader插件中读取的字段个数，否则该配置失效；
        - 映射关系按该配置中字段的先后顺序依次匹配；
    - 必选：否
    - 字段类型：String[]
    - 默认值：无

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
    - 描述：卡夫卡消费的启动模式，有效值为'earliest-offset'，'latest-offset'，'group-offsets'，'timestamp'和'specific-offsets'。有关更多详细信息，请参阅以下[开始阅读位置](https://ci.apache.org/projects/flink/flink-docs-release-1.12/zh/dev/table/connectors/kafka.html#start-reading-position)。
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
-  **sink.semantic**
    - 描述：定义 Kafka 接收器的交付语义。有效的枚举是'at-least-once','exactly-once'和'none'。有关更多详细信息，请参阅[一致性保证](https://ci.apache.org/projects/flink/flink-docs-release-1.12/zh/dev/table/connectors/kafka.html#consistency-guarantees)。
    - 必选：否
    - 字段类型：String
    - 默认值：at-least-once
-  **sink.parallelism**
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

### Sql
upsert-kafka
```sql
CREATE TABLE pageviews_per_region (
  id BIGINT,
  col_bit BOOLEAN,
  col_tinyint BIGINT,
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka-x',
  'topic' = 'pageviews_per_region_2',
  'properties.bootstrap.servers' = 'localhost:9092',
  'key.format' = 'json',
    'value.format' = 'json'
);

CREATE TABLE pageviews (
  id BIGINT,
  col_bit BOOLEAN,
  col_tinyint BIGINT
) WITH (
  'connector' = 'kafka-x',
  'topic' = 'pageviews_2',
  'properties.bootstrap.servers' = 'localhost:9092',
    'value.format' = 'debezium-json'
);

-- 计算 pv、uv 并插入到 upsert-kafka sink
INSERT INTO pageviews_per_region
SELECT
  id,
  col_bit,
 col_tinyint
FROM pageviews;


-- {"before":null,"after":{"id":1,"col_bit":true,"col_tinyint":1},"op":"c"}
-- {"before":{"id":1,"col_bit":true,"col_tinyint":1},"after":{"id":1,"col_bit":true,"col_tinyint":2},"op":"u"}
-- {"before":{"id":2,"col_bit":true,"col_tinyint":2},"after":null,"op":"d"}
```
