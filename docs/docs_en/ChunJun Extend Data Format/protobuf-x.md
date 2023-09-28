# ProtoBuf Format

## 一、介绍

protobuf format允许读写protocol buffer数据，目前仅支持sql

## 二、支持版本

protocol buffer2.x、protocol buffer3.x


## 三、format名称

protobuf-x

## 四、如何写一个protobuf format相关的DDL

```sql
CREATE TABLE proto (
                               user_id BIGINT,
                               item_id BIGINT,
                               category_id BIGINT,
                               behavior STRING,
                               ts BIGINT
) WITH (
      'connector' = 'kafka',
      'topic' = 'user_behavior',
      'properties.bootstrap.servers' = 'localhost:9092',
      'properties.group.id' = 'testGroup',
      'format' = 'protobuf-x',
      'protobuf-x.message-class-name' = 'com.dtstack.chunjun.behavior.behaviorOuterClass$Message'
      )
```

## 五、format配置项

| 配置项                        | 是否必填 | 默认值 | 数据类型 | 描述                                                         |
| ----------------------------- | -------- | ------ | -------- | ------------------------------------------------------------ |
| protobuf-x.message-class-name | 是       | 无     | String   | protobuf消息体在proto文件生成的outerClass类的中对应的类的全路径 |

## 六、数据类型映射

| Flink SQL type                       | ProtobufType                                                 |
| ------------------------------------ | ------------------------------------------------------------ |
| STRINGSTRING                         | string、enumstring、enum                                     |
| BOOLEANBOOLEAN                       | boolbool                                                     |
| BINARY / VARBINARYBINARY / VARBINARY |                                                              |
| BYTES                                | bytes                                                        |
| DECIMAL                              |                                                              |
| TINYINT                              |                                                              |
| SMALLINT                             |                                                              |
| INT                                  | int32、uint32、sint32、fixedint32、sfixed32                  |
| BIGINT                               | int64、uint64、sint64、fixedint64、sfixed64                  |
| FLOAT                                | float                                                        |
| DOUBLE                               | double                                                       |
| DATE                                 |                                                              |
| TIME                                 |                                                              |
| TIMESTAMP                            |                                                              |
| ARRAY                                | repeated修饰符                                               |
| MAP                                  | map(key，只能是除了浮点型和字节之外的标量类型；value，不能是map)https://developers.google.com/protocol-buffers/docs/proto3#maps |
| MULTISET                             |                                                              |
| ROW                                  | nested，oneOf（Row的第一项为case）                           |

## Example

### proto文件

```protobuf
syntax = "proto3";

package ZPMC.Message;

message RepeatedBool {repeated bool Values = 1;}

message RepeatedMyBool {repeated bool Values = 1;}

message RepeatedInt32 {repeated int32 Values = 1;}

message RepeatedUint32 {repeated uint32 Values = 1;}

message RepeatedInt64 {repeated int64 Values = 1;}

message RepeatedUint64 {repeated uint64 Values = 1;}

message RepeatedFloat {repeated float Values = 1;}

message RepeatedDouble {repeated double Values = 1;}

message RepeatedString {repeated string Values = 1;}

message Variant{
  oneof Value{
    bool ValueBool = 1;
    RepeatedBool ArrayBool = 2;
    int32 ValueInt32 = 3;
    RepeatedInt32 ArrayInt32 = 4;
    uint32 ValueUint32 = 5;
    RepeatedUint32 ArrayUint32 = 6;
    int64 ValueInt64 = 7;
    RepeatedInt64 ArrayInt64 = 8;
    uint64 ValueUint64 = 9;
    RepeatedUint64 ArrayUint64 = 10;
    float ValueFloat = 11;
    RepeatedFloat ArrayFloat = 12;
    double ValueDouble = 13;
    RepeatedDouble arrayDouble=14;
    string ValueString = 15;
    RepeatedString ArrayString = 16;
    bytes ValueBytes = 17;
    int64 ValueTimestamp = 18;
  };
  bool boolx = 19;
  oneof Value2{
    bool ValueBool2 = 20;
    RepeatedBool ArrayBool2 = 21;
  }
  bool booly=22;
}

message MessageItem{
  string TagName = 1; //默认optional
  Variant TagValue = 2;
  int32   UaDataType = 3;
  bool   Quality = 4;
  int64 Timestamp = 5;
  map<string, string>TagInfos = 6;
  map<string, string>ExValues = 7;
}

message MessageGroup{
  map<string, string> GroupInfo = 1;
  repeated MessageItem Messages = 2;
}

```

### DDL

```sql
CREATE TABLE reader (
    GroupInfo MAP<STRING,STRING>
    ,Messages ARRAY<
          ROW<
                TAGNAME VARCHAR ,
                TagValue ROW<
                     `Value` ROW<
                        ValueCase INTEGER,
                        ValueBool BOOlEAN,
                        ArrayBool ROW<`Values` ARRAY<BOOLEAN>>,
                        ValueInt32 INTEGER,
                        ArrayInt32 ROW<`Values` ARRAY<INTEGER>>,
                        ValueUint32 INTEGER,
                        ArrayUint32 ROW<`Values` ARRAY<INTEGER>>,
                        ValueInt64 BIGINT,
                        ArrayInt64 ROW<`Values` ARRAY<BIGINT>>,
                        ValueUint64 BIGINT,
                        ArrayUint64 ROW<`Values` ARRAY<BIGINT>>,
                        ValueFloat FLOAT,
                        ArrayFloat ROW<`Values` ARRAY<FLOAT>>,
                        ValueDouble DOUBLE,
                        ArrayDouble ROW<`Values` ARRAY<DOUBLE>>,
                        ValueString STRING,
                        ArrayString ROW<`Values` ARRAY<STRING>>,
                        ValueBytes  BINARY,
                        ValueTimestamp  BIGINT
                     >,
                    boolx BOOLEAN,
                    Value2 ROW<
                         ValueCase INTEGER ,
                         Value2Value BOOLEAN,
                         ArrayBool2 ROW<`Values` ARRAY<BOOLEAN>>
  									>,
                    booly BOOLEAN
                >,
                UaDataType INTEGER,
                Quality BOOLEAN,
                `Timestamp` BIGINT,
                TagInfos MAP<STRING,STRING>,
                ExValues MAP<STRING,STRING>
            >
    >



--     , `topic` STRING METADATA VIRTUAL -- from Kafka connector
--     , `leader-epoch` int METADATA VIRTUAL -- from Kafka connector
--     , `offset` BIGINT METADATA VIRTUAL  -- from Kafka connector
--     , ts TIMESTAMP(3) METADATA FROM 'timestamp' -- from Kafka connector
--     , `timestamp-type` STRING METADATA VIRTUAL  -- from Kafka connector
--     , partition_id BIGINT METADATA FROM 'partition' VIRTUAL   -- from Kafka connector

) WITH (
      'connector' = 'kafka-x'
      ,'topic' = 'liuliu_proto_source'
      ,'properties.bootstrap.servers' = 'flink01:9092'
      ,'properties.group.id' = 'luna_g'
      ,'scan.startup.mode' = 'earliest-offset'
      ,'format' = 'protobuf-x'
      ,'protobuf-x.message-class-name' = 'ZPMC.Message.MessageGroupOuterClass$MessageGroup'
      ,'scan.parallelism' = '1'
);
```

