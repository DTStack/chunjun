/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.flinkx.connectors.kafka.table;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.config.StartupMode;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * @author chuixue
 * @create 2021-04-06 19:31
 * @description
 **/
public class KafkaDynamicSource extends org.apache.flink.streaming.connectors.kafka.table.KafkaDynamicSource {
    public KafkaDynamicSource(DataType physicalDataType, @Nullable DecodingFormat<DeserializationSchema<RowData>> keyDecodingFormat, DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat, int[] keyProjection, int[] valueProjection, @Nullable String keyPrefix, @Nullable List<String> topics, @Nullable Pattern topicPattern, Properties properties, StartupMode startupMode, Map<KafkaTopicPartition, Long> specificStartupOffsets, long startupTimestampMillis, boolean upsertMode) {
        super(physicalDataType, keyDecodingFormat, valueDecodingFormat, keyProjection, valueProjection, keyPrefix, topics, topicPattern, properties, startupMode, specificStartupOffsets, startupTimestampMillis, upsertMode);
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return valueDecodingFormat.getChangelogMode();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext context) {
        final DeserializationSchema<RowData> keyDeserialization =
                createDeserialization(context, keyDecodingFormat, keyProjection, keyPrefix);

        final DeserializationSchema<RowData> valueDeserialization =
                createDeserialization(context, valueDecodingFormat, valueProjection, null);

        final TypeInformation<RowData> producedTypeInfo =
                context.createTypeInformation(producedDataType);

        final FlinkKafkaConsumer<RowData> kafkaConsumer =
                createKafkaConsumer(keyDeserialization, valueDeserialization, producedTypeInfo);

        return SourceFunctionProvider.of(kafkaConsumer, false);
    }

    @Override
    public DynamicTableSource copy() {
        return null;
    }

    @Override
    public String asSummaryString() {
        return "DTStack kafka table source";
    }

    // --------------------------------------------------------------------------------------------

    @Override
    protected FlinkKafkaConsumer<RowData> createKafkaConsumer(
            DeserializationSchema<RowData> keyDeserialization,
            DeserializationSchema<RowData> valueDeserialization,
            TypeInformation<RowData> producedTypeInfo) {

        final DynamicKafkaDeserializationSchema.MetadataConverter[] metadataConverters =
                metadataKeys.stream()
                        .map(
                                k ->
                                        Stream.of(ReadableMetadata.values())
                                                .filter(rm -> rm.key.equals(k))
                                                .findFirst()
                                                .orElseThrow(IllegalStateException::new))
                        .map(m -> m.converter)
                        .toArray(DynamicKafkaDeserializationSchema.MetadataConverter[]::new);

        // check if connector metadata is used at all
        final boolean hasMetadata = metadataKeys.size() > 0;

        // adjust physical arity with value format's metadata
        final int adjustedPhysicalArity =
                producedDataType.getChildren().size() - metadataKeys.size();

        // adjust value format projection to include value format's metadata columns at the end
        final int[] adjustedValueProjection =
                IntStream.concat(
                        IntStream.of(valueProjection),
                        IntStream.range(
                                keyProjection.length + valueProjection.length,
                                adjustedPhysicalArity))
                        .toArray();

        final KafkaDeserializationSchema<RowData> kafkaDeserializer =
                new DynamicKafkaDeserializationSchema(
                        adjustedPhysicalArity,
                        keyDeserialization,
                        keyProjection,
                        valueDeserialization,
                        adjustedValueProjection,
                        hasMetadata,
                        metadataConverters,
                        producedTypeInfo,
                        upsertMode);

        final FlinkKafkaConsumer<RowData> kafkaConsumer;
        if (topics != null) {
            kafkaConsumer = new FlinkKafkaConsumer<>(topics, kafkaDeserializer, properties);
        } else {
            kafkaConsumer = new FlinkKafkaConsumer<>(topicPattern, kafkaDeserializer, properties);
        }

        switch (startupMode) {
            case EARLIEST:
                kafkaConsumer.setStartFromEarliest();
                break;
            case LATEST:
                kafkaConsumer.setStartFromLatest();
                break;
            case GROUP_OFFSETS:
                kafkaConsumer.setStartFromGroupOffsets();
                break;
            case SPECIFIC_OFFSETS:
                kafkaConsumer.setStartFromSpecificOffsets(specificStartupOffsets);
                break;
            case TIMESTAMP:
                kafkaConsumer.setStartFromTimestamp(startupTimestampMillis);
                break;
        }

        kafkaConsumer.setCommitOffsetsOnCheckpoints(properties.getProperty("group.id") != null);

        if (watermarkStrategy != null) {
            kafkaConsumer.assignTimestampsAndWatermarks(watermarkStrategy);
        }
        return kafkaConsumer;
    }

    private @Nullable DeserializationSchema<RowData> createDeserialization(
            DynamicTableSource.Context context,
            @Nullable DecodingFormat<DeserializationSchema<RowData>> format,
            int[] projection,
            @Nullable String prefix) {
        if (format == null) {
            return null;
        }
        DataType physicalFormatDataType =
                DataTypeUtils.projectRow(this.physicalDataType, projection);
        if (prefix != null) {
            physicalFormatDataType = DataTypeUtils.stripRowPrefix(physicalFormatDataType, prefix);
        }
        return format.createRuntimeDecoder(context, physicalFormatDataType);
    }

    // --------------------------------------------------------------------------------------------
    // Metadata handling
    // --------------------------------------------------------------------------------------------

    enum ReadableMetadata {
        TOPIC(
                "topic",
                DataTypes.STRING().notNull(),
                new DynamicKafkaDeserializationSchema.MetadataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object read(ConsumerRecord<?, ?> record) {
                        return StringData.fromString(record.topic());
                    }
                }),

        PARTITION(
                "partition",
                DataTypes.INT().notNull(),
                new DynamicKafkaDeserializationSchema.MetadataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object read(ConsumerRecord<?, ?> record) {
                        return record.partition();
                    }
                }),

        HEADERS(
                "headers",
                // key and value of the map are nullable to make handling easier in queries
                DataTypes.MAP(DataTypes.STRING().nullable(), DataTypes.BYTES().nullable())
                        .notNull(),
                new DynamicKafkaDeserializationSchema.MetadataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object read(ConsumerRecord<?, ?> record) {
                        final Map<StringData, byte[]> map = new HashMap<>();
                        for (Header header : record.headers()) {
                            map.put(StringData.fromString(header.key()), header.value());
                        }
                        return new GenericMapData(map);
                    }
                }),

        LEADER_EPOCH(
                "leader-epoch",
                DataTypes.INT().nullable(),
                new DynamicKafkaDeserializationSchema.MetadataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object read(ConsumerRecord<?, ?> record) {
                        return record.leaderEpoch().orElse(null);
                    }
                }),

        OFFSET(
                "offset",
                DataTypes.BIGINT().notNull(),
                new DynamicKafkaDeserializationSchema.MetadataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object read(ConsumerRecord<?, ?> record) {
                        return record.offset();
                    }
                }),

        TIMESTAMP(
                "timestamp",
                DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3).notNull(),
                new DynamicKafkaDeserializationSchema.MetadataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object read(ConsumerRecord<?, ?> record) {
                        return TimestampData.fromEpochMillis(record.timestamp());
                    }
                }),

        TIMESTAMP_TYPE(
                "timestamp-type",
                DataTypes.STRING().notNull(),
                new DynamicKafkaDeserializationSchema.MetadataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object read(ConsumerRecord<?, ?> record) {
                        return StringData.fromString(record.timestampType().toString());
                    }
                });

        final String key;

        final DataType dataType;

        final DynamicKafkaDeserializationSchema.MetadataConverter converter;

        ReadableMetadata(String key, DataType dataType, DynamicKafkaDeserializationSchema.MetadataConverter converter) {
            this.key = key;
            this.dataType = dataType;
            this.converter = converter;
        }
    }
}
