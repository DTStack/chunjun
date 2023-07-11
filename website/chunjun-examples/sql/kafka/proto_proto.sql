--complex with flink original kafka-connector
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
                         ArrayBool2 ROW<`Values` ARRAY<BOOLEAN>>>,
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
--       ,'protobuf-x.class.name'='ZPMC.Message.MessageGroupOuterClass'
--       ,'protobuf-x.message.name'='MessageGroup'
      ,'protobuf-x.message-class-name' = 'ZPMC.Message.MessageGroupOuterClass$MessageGroup'
      ,'scan.parallelism' = '1'
      );

CREATE TABLE writer (
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
                            ArrayBool2 ROW<`Values` ARRAY<BOOLEAN>>>,
                            booly BOOLEAN
                        >,
                        UaDataType INTEGER,
                        Quality BOOLEAN,
                        `Timestamp` BIGINT,
                        TagInfos MAP<STRING,STRING>,
                        ExValues MAP<STRING,STRING>
                    >
    >


--
--     , `topic` STRING METADATA VIRTUAL -- from Kafka connector
--     , `leader-epoch` int METADATA VIRTUAL -- from Kafka connector
--     , `offset` BIGINT METADATA VIRTUAL  -- from Kafka connector
--     , ts TIMESTAMP(3) METADATA FROM 'timestamp' -- from Kafka connector
--     , `timestamp-type` STRING METADATA VIRTUAL  -- from Kafka connector
--     , partition_id BIGINT METADATA FROM 'partition' VIRTUAL   -- from Kafka connector

) WITH (
      'connector' = 'kafka-x'
      ,'topic' = 'liuliu_proto_sink'
      ,'properties.bootstrap.servers' = 'flink01:9092'
      ,'properties.group.id' = 'luna_g'
      ,'scan.startup.mode' = 'earliest-offset'
      ,'format' = 'protobuf-x'
--       ,'protobuf-x.class.name'='ZPMC.Message.MessageGroupOuterClass'
--       ,'protobuf-x.message.name'='MessageGroup'
      ,'protobuf-x.message-class-name' = 'ZPMC.Message.MessageGroupOuterClass$MessageGroup'
      ,'scan.parallelism' = '1'
      );

INSERT INTO writer
SELECT *
from reader

