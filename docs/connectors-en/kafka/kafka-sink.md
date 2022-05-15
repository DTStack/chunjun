# Kafka Sink

## 1. Introduce

kafka sink

## 2. Version Support
Kafka mainstream version

## 3. Connector Name

| Sync | kafkasink、kafkawriter |
| --- | --- |
| SQL | kafka-x |

## 4. Parameter description

### 4.1. Sync

- **topic**
    - Description：Topic name of the Kafka record.
    - Required：required
    - Type：String
    - Default：(none)
      <br />

- **consumerSettings**
    - Description：This can set and pass arbitrary Kafka configurations. It supports all options in `kafka.consumer.ConsumerConfig.ConsumerConfig` class.
    - Required：required
    - Type：Map
    - Default：(none)
    - Example：
      ```json
      {
          "consumerSettings":{
              "bootstrap.servers":"host1:9092,host2:9092,host3:9092"
          }
      }
      ```

<br />

- **tableFields**
    - Description：reader and writer fields mapping。If this option is set, the key of JSON will subject to it.
    - Note：
        - If this option is set, the number of writer column can't less than reader. Otherwise, the configuration will be disabled.
        - The mapping relationship is matched according to the sequence of fields.
- Required：optional
    - Type：String[]
    - Default：(none)
      <br />

### 4.2. SQL

The details are in [kafka-connector](https://ci.apache.org/projects/flink/flink-docs-release-1.12/dev/table/connectors/kafka.html)

- **connector**
    - Description：kafka-x
    - Required：required
    - Type：String
    - Default：(none)
      <br />

- **topic**
    - Description：Topic name(s) to read data from when the table is used as source. It also supports topic list for source by separating topic by semicolon like 'topic-1;topic-2'. Note, only one of "topic-pattern" and "topic" can be specified for sources. When the table is used as sink, the topic name is the topic to write data to. Note topic list is not supported for sinks.
    - Required：required
    - Type：String
    - Default：(none)
      <br />

- **topic-pattern**
    - Description：The regular expression for a pattern of topic names to read from. All topics with names that match the specified regular expression will be subscribed by the consumer when the job starts running. Note, only one of "topic-pattern" and "topic" can be specified for sources.
    - Required：optional
    - Type：String
    - Default：(none)
      <br />

- **properties.bootstrap.servers**
    - Description：Comma separated list of Kafka brokers.
    - Required：required
    - Type：String
    - Default：(none)
      <br />

- **properties.group.id**
    - Description：The id of the consumer group for Kafka source, optional for Kafka sink.
    - Required：required by source
    - Type：String
    - Default：(none)
      <br />

- **properties.***
    - Description：This can set and pass arbitrary Kafka configurations. Suffix names must match the configuration key defined in [Kafka Configuration documentation](https://kafka.apache.org/documentation/#configuration). Flink will remove the "properties." key prefix and pass the transformed key and values to the underlying KafkaClient. For example, you can disable automatic topic creation via 'properties.allow.auto.create.topics' = 'false'. But there are some configurations that do not support to set, because Flink will override them, e.g. 'key.deserializer' and 'value.deserializer'.
    - Required：optional
    - Type：String
    - Default：(none)
      <br />

- **format**
    - Description: The format used to deserialize and serialize the value part of Kafka messages. Please refer to the [format](https://ci.apache.org/projects/flink/flink-docs-release-1.12/zh/dev/table/connectors/formats/) page for more details and more format options. Note: Either this option or the 'value.format' option are required.
    - Required：required
    - Type：String
    - Default：(none)
      <br />

- **key.format**
    - Description：The format used to deserialize and serialize the key part of Kafka messages. Please refer to the [formats](https://ci.apache.org/projects/flink/flink-docs-release-1.12/zh/dev/table/connectors/formats/) page for more details and more format options. Note: If a key format is defined, the 'key.fields' option is required as well. Otherwise the Kafka records will have an empty key.
    - Required：optional
    - Type：String
    - Default：(none)
      <br />

- **key.fields**
    - Description：Defines an explicit list of physical columns from the table schema that configure the data type for the key format. By default, this list is empty and thus a key is undefined. The list should look like 'field1;field2'.
    - Required：optional
    - Type：List<String>
    - Default：(none)
      <br />

- **key.fields-prefix**
    - Description：Defines a custom prefix for all fields of the key format to avoid name clashes with fields of the value format. By default, the prefix is empty. If a custom prefix is defined, both the table schema and 'key.fields' will work with prefixed names. When constructing the data type of the key format, the prefix will be removed and the non-prefixed names will be used within the key format. Please note that this option requires that 'value.fields-include' must be set to 'EXCEPT_KEY'.
    - Required：optional
    - Type：String
    - Default：(none)
      <br />

- **value.format**
    - Description：The format used to deserialize and serialize the value part of Kafka messages. Please refer to the formats page for more details and more format options. Note: Either this option or the ['format'](https://ci.apache.org/projects/flink/flink-docs-release-1.12/zh/dev/table/connectors/formats/) option are required.
    - Required：required
    - Type：String
    - Default：(none)
      <br />

- **value.fields-include**
    - Description：Defines a strategy how to deal with key columns in the data type of the value format. By default, 'ALL' physical columns of the table schema will be included in the value format which means that key columns appear in the data type for both the key and value format.
    - Required：optional
    - Type：Enum
        - Possible values: [ALL, EXCEPT_KEY]
    - Default：ALL
      <br />

- **sink.partitioner**
    - Description：String Output partitioning from Flink's partitions into Kafka's partitions. Valid values are default: use the kafka default partitioner to partition records. fixed: each Flink partition ends up in at most one Kafka partition. round-robin: a Flink partition is distributed to Kafka partitions sticky round-robin. It only works when record's keys are not specified. Custom FlinkKafkaPartitioner subclass: e.g. 'org.mycompany.MyPartitioner'.See the following [Sink Partitioning](https://ci.apache.org/projects/flink/flink-docs-release-1.12/zh/dev/table/connectors/kafka.html#sink-partitioning) for more details.
    - Required：optional
    - Type：String
    - Default：default
      <br />

- **sink.semantic**
    - Description：Defines the delivery semantic for the Kafka sink. Valid enumerationns are 'at-least-once', 'exactly-once' and 'none'.
      See [Consistency guarantees](https://ci.apache.org/projects/flink/flink-docs-release-1.12/zh/dev/table/connectors/kafka.html#consistency-guarantees) for more details.
    - Required：optional
    - Type：String
    - Default：at-least-once
      <br />

- **sink.parallelism**
    - Description：Defines the parallelism of the Kafka sink operator. By default, the parallelism is determined by the framework using the same parallelism of the upstream chained operator.
    - Required：optional
    - Type：Integer
    - Default：(none)
      <br />

## 5. Data Type

| support | BOOLEAN、TINYINT、SMALLINT、INT、BIGINT、FLOAT、DOUBLE、DECIMAL、STRING、VARCHAR、CHAR、TIMESTAMP、DATE、BINARY、ARRAY、MAP、STRUCT、LIST、ROW |
| --- | --- |
| no support | others |

## 6. Example

The details are in `chunjun-examples` dir.
