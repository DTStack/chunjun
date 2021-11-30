# Kafka Source

## 1. Introduce

Kafka Source

## 2. Version Support

Kafka mainstream version

## 3. Connector Name

| Sync | kafkasource、kafkareader |
| --- | --- |
| SQL | kafka-x |

## 4. Parameter description

### 4.1. Sync

- **topic**
    - Description: Topic name of the Kafka record. It also supports topic list for source by separating topic by semicolon like 'topic-1;topic-2'. When mode option is `timestamp` or `specific-offsets`, topic list isn't supported.
    - Requested: required
    - Type: String
    - Default：(none)
      <br />

- **mode**
    - Description: Startup mode for Kafka consumer, valid values are 'earliest-offset', 'latest-offset', 'group-offsets', 'timestamp' and 'specific-offsets'. See the following [Start Reading Position](https://ci.apache.org/projects/flink/flink-docs-release-1.12/dev/table/connectors/kafka.html#start-reading-position) for more details.
    - Requested: optional
    - Type: String
    - Default：group-offsets
      <br />

- **timestamp**
    - Description: Start from user-supplied timestamp for each partition. only `kafkareader`
    - Requested: It's requested, when the mode option is timestamp.
    - Type: Long
    - Default：(none)
      <br />

- **offset**
    - Description: Start from user-supplied specific offsets for each partition. only `kafkareader`
    - Requested: It's requested, when the mode option is timestamp.
    - Type: String
    - Format: partition:0,offset:42;partition:1,offset:300;partition:2,offset:300
    - Default：(none)
      <br />

- **groupId**
    - Description: The id of the consumer group for Kafka.
    - Requested: optional
    - Type: String
    - Default：default
      <br />

- **encoding**
    - Description: character encoding
    - Requested: optional
    - Type: String
    - Default：UTF-8
      <br />

- **codec**
    - Description: type of message format. Valid values are 'json', 'text'.
        - text：It will put kafka record to a map, which map's key is 'message'. For example message in kafka is {"key":"key","message":"value"}
          the data format sent to the downstream is:
          ```json
          [
              {
                  "message":"{\"key\": \"key\", \"value\": \"value\"}"
              }
          ]
          ```
    - json：Treat message as JSON format
         - If record include message key, the data format sent to the downstream is:
              ```json
              [
                  {
                      "key1":"value1",
                      "message":"value"
                  }
              ]
              ```
        - When the message field isn't included, it will add a key and value. The data format sent to the downstream is:
          ```json
          [
              {
                  "key1":"value1",
                  "key2":"value2",
                  "message":"{\"key1\": \"value1\", \"key2\": \"value2\"}"
              }
          ]
          ```
        - It will be treated as text, if message isn't JSON format.
    - Requested: optional
    - Type: String
    - Default：text
      <br />

- **consumerSettings**
    - Description: This can set and pass arbitrary Kafka configurations. It supports all options in `kafka.consumer.ConsumerConfig.ConsumerConfig` class.
    - Requested: required
    - Type: Map
    - Default：(none)
    - 如：
    ```json
    {
        "consumerSettings":{
            "bootstrap.servers":"host1:9092,host2:9092,host3:9092"
        }
    }
    ```

- **column**
    - Description: Field type mapping for writer 
    - Requested: optional
    - Type: List
    - Default：(none)
    - Note：each column options description:
        - name：field name
        - type：field type. It could be different from writer field type, FlinkX can auto convert.
        - format： If field type is time attribute, It could be set time format, and auto convert from string type to timestamp type.
        - for example：
          ```json
          "column": [{
                "name": "col",
                "type": "datetime",
                "format": "yyyy-MM-dd hh:mm:ss"
            }]
          ```

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

- **scan.startup.mode**
    - Description: Startup mode for Kafka consumer, valid values are 'earliest-offset', 'latest-offset', 'group-offsets', 'timestamp' and 'specific-offsets'. See the following [Start Reading Position](https://ci.apache.org/projects/flink/flink-docs-release-1.12/zh/dev/table/connectors/kafka.html#start-reading-position) for more details.
    - Requested: optional
    - Type: String
    - Default：group-offsets
      <br />

- **scan.startup.specific-offsets**
    - Description: Specify offsets for each partition in case of 'specific-offsets' startup mode, e.g. 'partition:0,offset:42;partition:1,offset:300'.
    - Requested: optional
    - Type: String
    - Default：(none)
      <br />

- **scan.startup.timestamp-millis**
    - Description: Start from the specified epoch timestamp (milliseconds) used in case of 'timestamp' startup mode.
    - Requested: optional
    - Type: Long
    - Default：(none)
      <br />

- **scan.topic-partition-discovery.interval**
    - Description: Interval for consumer to discover dynamically created Kafka topics and partitions periodically.
    - Requested: optional
    - Type: Duration
    - Default：(none)
      <br />

## 5. Data Type

| support | BOOLEAN、TINYINT、SMALLINT、INT、BIGINT、FLOAT、DOUBLE、DECIMAL、STRING、VARCHAR、CHAR、TIMESTAMP、DATE、BINARY、ARRAY、MAP、STRUCT、LIST、ROW |
| --- | --- |
| no support | others |

## 6. Example

The details are in `flinkx-examples` dir.
