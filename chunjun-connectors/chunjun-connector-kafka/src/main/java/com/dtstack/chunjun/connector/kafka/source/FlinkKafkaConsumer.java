/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dtstack.chunjun.connector.kafka.source;

import com.dtstack.chunjun.connector.kafka.util.KafkaUtil;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.config.OffsetCommitMode;
import org.apache.flink.streaming.connectors.kafka.internals.AbstractFetcher;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaFetcher;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.util.SerializedValue;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

public class FlinkKafkaConsumer<T>
        extends org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer<T> {

    private final DynamicKafkaDeserializationSchema deserializationSchema;

    /**
     * Creates a new Kafka streaming source consumer.
     *
     * <p>Flink's objects.
     *
     * @param props will be modified props
     * @param originalProps original props
     */
    public FlinkKafkaConsumer(
            List<String> topics,
            KafkaDeserializationSchema<T> deserializer,
            Properties props,
            Properties originalProps,
            DynamicKafkaDeserializationSchema deserializationSchema) {
        super(topics, deserializer, props);
        this.deserializationSchema = deserializationSchema;
        modifyProps(originalProps);
    }

    /**
     * Creates a new Kafka streaming source consumer. Use this constructor to subscribe to multiple
     * topics based on a regular expression pattern.
     *
     * <p>If partition discovery is enabled (by setting a non-negative value for {@link
     * org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer#KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS}
     * in the properties), topics with names matching the pattern will also be subscribed to as they
     * are created on the fly.
     *
     * @param subscriptionPattern The regular expression for a pattern of topic names to subscribe
     *     to. Flink's objects.
     * @param props will be modified props
     * @param originalProps original props
     */
    public FlinkKafkaConsumer(
            Pattern subscriptionPattern,
            KafkaDeserializationSchema<T> deserializer,
            Properties props,
            Properties originalProps,
            DynamicKafkaDeserializationSchema deserializationSchema) {
        super(subscriptionPattern, deserializer, props);
        this.deserializationSchema = deserializationSchema;
        modifyProps(originalProps);
    }

    @Override
    protected AbstractFetcher<T, ?> createFetcher(
            SourceContext<T> sourceContext,
            Map<KafkaTopicPartition, Long> assignedPartitionsWithInitialOffsets,
            SerializedValue<WatermarkStrategy<T>> watermarkStrategy,
            StreamingRuntimeContext runtimeContext,
            OffsetCommitMode offsetCommitMode,
            MetricGroup consumerMetricGroup,
            boolean useMetrics)
            throws Exception {

        // make sure that auto commit is disabled when our offset commit mode is ON_CHECKPOINTS;
        // this overwrites whatever setting the user configured in the properties
        adjustAutoCommitConfig(properties, offsetCommitMode);

        KafkaFetcher<T> kafkaFetcher =
                new KafkaFetcher<>(
                        sourceContext,
                        assignedPartitionsWithInitialOffsets,
                        watermarkStrategy,
                        runtimeContext.getProcessingTimeService(),
                        runtimeContext.getExecutionConfig().getAutoWatermarkInterval(),
                        runtimeContext.getUserCodeClassLoader(),
                        runtimeContext.getTaskNameWithSubtasks(),
                        deserializer,
                        properties,
                        pollTimeout,
                        runtimeContext.getMetricGroup(),
                        consumerMetricGroup,
                        useMetrics);

        KafkaUtil.registerLagMetrics(
                kafkaFetcher, deserializationSchema.inputMetric.getChunjunMetricGroup());
        return kafkaFetcher;
    }

    private void modifyProps(Properties originalProps) {
        for (Map.Entry<Object, Object> entry : originalProps.entrySet()) {
            super.properties.setProperty(
                    String.valueOf(entry.getKey()), String.valueOf(entry.getValue()));
        }
    }
}
