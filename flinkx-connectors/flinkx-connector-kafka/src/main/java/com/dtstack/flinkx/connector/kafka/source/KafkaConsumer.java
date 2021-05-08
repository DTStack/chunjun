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

package com.dtstack.flinkx.connector.kafka.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.config.OffsetCommitMode;
import org.apache.flink.streaming.connectors.kafka.internals.AbstractFetcher;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.SerializedValue;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * @author chuixue
 * @create 2021-05-07 14:46
 * @description
 */
public class KafkaConsumer extends FlinkKafkaConsumer<RowData>{

    private final KafkaDeserializationSchema<RowData> deserializationSchema;

    public KafkaConsumer(List<String> topics, KafkaDeserializationSchema<RowData> deserializer, Properties props) {
        super(topics, deserializer, props);
        this.deserializationSchema = deserializer;
    }

    public KafkaConsumer(
            Pattern subscriptionPattern,
            KafkaDeserializationSchema<RowData> deserializer,
            Properties props) {
        super(subscriptionPattern, deserializer, props);
        this.deserializationSchema = deserializer;
    }

    @Override
    public void run(SourceContext<RowData> sourceContext) throws Exception {
        ((DynamicKafkaDeserializationSchemaWrapper) deserializationSchema).setRuntimeContext(getRuntimeContext());
        ((DynamicKafkaDeserializationSchemaWrapper) deserializationSchema).initMetric();
        super.run(sourceContext);
    }

    @Override
    protected AbstractFetcher<RowData, ?> createFetcher(
            SourceContext<RowData> sourceContext,
            Map<KafkaTopicPartition, Long> assignedPartitionsWithInitialOffsets,
            SerializedValue<WatermarkStrategy<RowData>> watermarkStrategy,
            StreamingRuntimeContext runtimeContext,
            OffsetCommitMode offsetCommitMode,
            MetricGroup consumerMetricGroup,
            boolean useMetrics)
            throws Exception {
        AbstractFetcher<RowData, ?> fetcher =
                super.createFetcher(
                        sourceContext,
                        assignedPartitionsWithInitialOffsets,
                        watermarkStrategy,
                        runtimeContext,
                        offsetCommitMode,
                        consumerMetricGroup,
                        useMetrics);

        ((DynamicKafkaDeserializationSchemaWrapper) deserializationSchema).setFetcher(fetcher);
        return fetcher;
    }
}
