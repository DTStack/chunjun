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
package com.dtstack.chunjun.connector.kafka.util;

import com.dtstack.chunjun.connector.kafka.deserializer.DtKafkaDeserializer;
import com.dtstack.chunjun.constants.ConstantValue;
import com.dtstack.chunjun.constants.Metrics;
import com.dtstack.chunjun.util.ReflectionUtils;

import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.connectors.kafka.internals.AbstractFetcher;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaConsumerThread;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartitionState;
import org.apache.flink.streaming.connectors.kafka.internals.metrics.KafkaConsumerMetricConstants;
import org.apache.flink.streaming.connectors.kafka.table.KafkaOptions;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.internals.SubscriptionState;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.requests.IsolationLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Date: 2020/12/31 Company: www.dtstack.com
 *
 * @author tudou
 */
public class KafkaUtil {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaUtil.class);

    /**
     * 解析kafka offset字符串
     *
     * @param topic
     * @param offsetString
     * @return
     * @throws IllegalArgumentException
     */
    public static Map<KafkaTopicPartition, Long> parseSpecificOffsetsString(
            String topic, String offsetString) throws IllegalArgumentException {
        final String[] pairs = offsetString.split(ConstantValue.SEMICOLON_SYMBOL);
        final String validationExceptionMessage =
                "Invalid properties [offset] should follow the format 'partition:0,offset:42;partition:1,offset:300', but is '"
                        + offsetString
                        + "';";

        if (pairs.length == 0) {
            throw new IllegalArgumentException(validationExceptionMessage);
        }

        Map<KafkaTopicPartition, Long> map = new HashMap<>();
        for (String pair : pairs) {
            if (null == pair || !pair.contains(ConstantValue.COMMA_SYMBOL)) {
                throw new IllegalArgumentException(validationExceptionMessage);
            }

            final String[] kv = pair.split(ConstantValue.COMMA_SYMBOL);
            if (kv.length != 2 || !kv[0].startsWith("partition:") || !kv[1].startsWith("offset:")) {
                throw new IllegalArgumentException(validationExceptionMessage);
            }

            String partitionValue = kv[0].substring(kv[0].indexOf(ConstantValue.COLON_SYMBOL) + 1);
            String offsetValue = kv[1].substring(kv[1].indexOf(ConstantValue.COLON_SYMBOL) + 1);
            try {
                final int partition = Integer.parseInt(partitionValue);
                final Long offset = Long.valueOf(offsetValue);
                map.put(new KafkaTopicPartition(topic, partition), offset);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(validationExceptionMessage, e);
            }
        }
        return map;
    }

    public static Properties getKafkaProperties(Map<String, String> tableOptions) {
        final Properties kafkaProperties = new Properties();
        boolean hasKafkaClientProperties =
                tableOptions.keySet().stream()
                        .anyMatch(k -> k.startsWith(KafkaOptions.PROPERTIES_PREFIX));
        if (hasKafkaClientProperties) {
            tableOptions.keySet().stream()
                    .filter(key -> key.startsWith(KafkaOptions.PROPERTIES_PREFIX))
                    .forEach(
                            key -> {
                                final String value = tableOptions.get(key);
                                final String subKey =
                                        key.substring((KafkaOptions.PROPERTIES_PREFIX).length());
                                kafkaProperties.put(subKey, value);
                            });
            String keyDeserializer = tableOptions.get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG);
            if (StringUtils.isNotBlank(keyDeserializer)) {
                kafkaProperties.put(
                        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                        DtKafkaDeserializer.class.getName());
                kafkaProperties.put("dt.key.deserializer", keyDeserializer);
            }
            String valueDeserializer =
                    tableOptions.get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG);
            if (StringUtils.isNotBlank(valueDeserializer)) {
                kafkaProperties.put(
                        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                        DtKafkaDeserializer.class.getName());
                kafkaProperties.put("dt.value.deserializer", valueDeserializer);
            }
        }
        return kafkaProperties;
    }

    @SuppressWarnings("all")
    public static void registerLagMetrics(
            AbstractFetcher kafkaFetcher, MetricGroup chunjunMetricGroup) throws Exception {
        Field consumerThreadField =
                ReflectionUtils.getDeclaredField(kafkaFetcher, "consumerThread");
        consumerThreadField.setAccessible(true);
        KafkaConsumerThread consumerThread =
                (KafkaConsumerThread) consumerThreadField.get(kafkaFetcher);

        Field hasAssignedPartitionsField =
                consumerThread.getClass().getDeclaredField("hasAssignedPartitions");
        hasAssignedPartitionsField.setAccessible(true);

        // get subtask unassigned kafka topic partition
        Field subscribedPartitionStatesField =
                ReflectionUtils.getDeclaredField(kafkaFetcher, "subscribedPartitionStates");
        subscribedPartitionStatesField.setAccessible(true);
        List<KafkaTopicPartitionState<?, KafkaTopicPartition>> subscribedPartitionStates =
                (List<KafkaTopicPartitionState<?, KafkaTopicPartition>>)
                        subscribedPartitionStatesField.get(kafkaFetcher);
        // init partition lag metric
        for (KafkaTopicPartitionState<?, KafkaTopicPartition> kafkaTopicPartitionState :
                subscribedPartitionStates) {
            KafkaTopicPartition kafkaTopicPartition =
                    kafkaTopicPartitionState.getKafkaTopicPartition();
            MetricGroup kafkaConsumerGroup =
                    chunjunMetricGroup.addGroup(
                            KafkaConsumerMetricConstants.KAFKA_CONSUMER_METRICS_GROUP);

            MetricGroup topicMetricGroup =
                    kafkaConsumerGroup.addGroup(
                            KafkaConsumerMetricConstants.OFFSETS_BY_TOPIC_METRICS_GROUP,
                            kafkaTopicPartition.getTopic());
            MetricGroup finalMetricGroup =
                    topicMetricGroup.addGroup(
                            KafkaConsumerMetricConstants.OFFSETS_BY_PARTITION_METRICS_GROUP,
                            kafkaTopicPartition.getPartition() + "");
            finalMetricGroup.gauge(
                    Metrics.LAG_GAUGE,
                    new Gauge<Long>() {
                        // tmp variable
                        boolean initLag = true;
                        int partitionIndex;
                        SubscriptionState subscriptionState;
                        TopicPartition topicPartition;

                        @Override
                        public Long getValue() {
                            // first time register metrics
                            if (initLag) {
                                partitionIndex = kafkaTopicPartition.getPartition();
                                initLag = false;
                                return -1L;
                            }
                            // when kafka topic partition assigned calc metrics
                            if (subscriptionState == null) {
                                try {
                                    Field consumerField =
                                            consumerThread.getClass().getDeclaredField("consumer");
                                    consumerField.setAccessible(true);

                                    KafkaConsumer kafkaConsumer =
                                            (KafkaConsumer) consumerField.get(consumerThread);
                                    Field subscriptionStateField =
                                            kafkaConsumer
                                                    .getClass()
                                                    .getDeclaredField("subscriptions");
                                    subscriptionStateField.setAccessible(true);

                                    boolean hasAssignedPartitions =
                                            (boolean)
                                                    hasAssignedPartitionsField.get(consumerThread);

                                    if (!hasAssignedPartitions) {
                                        LOG.error("wait 50 secs, but not assignedPartitions");
                                    }

                                    subscriptionState =
                                            (SubscriptionState)
                                                    subscriptionStateField.get(kafkaConsumer);

                                    topicPartition =
                                            subscriptionState.assignedPartitions().stream()
                                                    .filter(x -> x.partition() == partitionIndex)
                                                    .findFirst()
                                                    .get();

                                } catch (Exception e) {
                                    LOG.error("", e.getMessage());
                                }
                                return -1L;
                            } else {
                                return subscriptionState.partitionLag(
                                        topicPartition, IsolationLevel.READ_UNCOMMITTED);
                            }
                        }
                    });
        }
    }
}
