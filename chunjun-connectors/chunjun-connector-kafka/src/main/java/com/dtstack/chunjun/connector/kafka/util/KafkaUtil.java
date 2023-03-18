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
import org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptionsUtil;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.internals.SubscriptionState;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Date: 2020/12/31 Company: www.dtstack.com
 *
 * @author tudou
 */
public class KafkaUtil {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaUtil.class);

    private static final String KEY_PARTITIONS = "partitions";

    private static final String KEY_REPLICAS = "replicas";

    /**
     * 解析kafka offset字符串
     *
     * @param topics
     * @param offsetString
     * @return
     * @throws IllegalArgumentException
     */
    public static Map<KafkaTopicPartition, Long> parseSpecificOffsetsString(
            List<String> topics, String offsetString) throws IllegalArgumentException {
        final String[] pairs = offsetString.split(ConstantValue.SEMICOLON_SYMBOL);
        final String validationExceptionMessage =
                "Invalid properties [offset] should follow the format 'partition:0,offset:42;partition:1,offset:300', but is '"
                        + offsetString
                        + "';";

        final String validationExceptionMessageForTopics =
                "Invalid properties [offset] should follow the format 'topic:topic1,partition:0,offset:42;partition:1,offset:300', but is '"
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
            if (topics.size() == 1
                    && (kv.length != 2
                            || !kv[0].startsWith("partition:")
                            || !kv[1].startsWith("offset:"))) {
                throw new IllegalArgumentException(validationExceptionMessage);
            } else if (topics.size() > 1
                    && (kv.length != 3
                            || !kv[0].startsWith("topic:")
                            || !kv[1].startsWith("partition:")
                            || !kv[2].startsWith("offset:"))) {
                throw new IllegalArgumentException(validationExceptionMessageForTopics);
            }

            if (topics.size() == 1) {
                String partitionValue =
                        kv[0].substring(kv[0].indexOf(ConstantValue.COLON_SYMBOL) + 1);
                String offsetValue = kv[1].substring(kv[1].indexOf(ConstantValue.COLON_SYMBOL) + 1);
                try {
                    final int partition = Integer.parseInt(partitionValue);
                    final Long offset = Long.valueOf(offsetValue);
                    map.put(new KafkaTopicPartition(topics.get(0), partition), offset);
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException(validationExceptionMessage, e);
                }
            } else {
                String topic = kv[0].substring(kv[0].indexOf(ConstantValue.COLON_SYMBOL) + 1);
                String partitionValue =
                        kv[1].substring(kv[1].indexOf(ConstantValue.COLON_SYMBOL) + 1);
                String offsetValue = kv[2].substring(kv[2].indexOf(ConstantValue.COLON_SYMBOL) + 1);
                try {
                    final int partition = Integer.parseInt(partitionValue);
                    final Long offset = Long.valueOf(offsetValue);
                    map.put(new KafkaTopicPartition(topic, partition), offset);
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException(validationExceptionMessage, e);
                }
            }
        }
        return map;
    }

    public static Properties getKafkaProperties(Map<String, String> tableOptions) {
        final Properties kafkaProperties = new Properties();
        boolean hasKafkaClientProperties =
                tableOptions.keySet().stream()
                        .anyMatch(k -> k.startsWith(KafkaConnectorOptionsUtil.PROPERTIES_PREFIX));
        if (hasKafkaClientProperties) {
            tableOptions.keySet().stream()
                    .filter(key -> key.startsWith(KafkaConnectorOptionsUtil.PROPERTIES_PREFIX))
                    .forEach(
                            key -> {
                                final String value = tableOptions.get(key);
                                final String subKey =
                                        key.substring(
                                                (KafkaConnectorOptionsUtil.PROPERTIES_PREFIX)
                                                        .length());
                                kafkaProperties.put(subKey, value);
                            });
            String keyDeserializer = tableOptions.get("key.deserializer");
            if (StringUtils.isNotBlank(keyDeserializer)) {
                kafkaProperties.put(
                        "key.deserializer",
                        "com.dtstack.chunjun.connector.kafka.deserializer.DtKafkaDeserializer");
                kafkaProperties.put("dt.key.deserializer", keyDeserializer);
            }
            String valueDeserializer = tableOptions.get("value.deserializer");
            if (StringUtils.isNotBlank(valueDeserializer)) {
                kafkaProperties.put(
                        "value.deserializer",
                        "com.dtstack.chunjun.connector.kafka.deserializer.DtKafkaDeserializer");
                kafkaProperties.put("dt.value.deserializer", valueDeserializer);
            }
        }
        return kafkaProperties;
    }

    /** 初始化metaData Kafka配置参数 */
    public static Properties getMetaDataKafkaProperties(Map<String, String> consumerSettings) {
        LOG.info(
                "Initialize Kafka configuration information, consumerSettings : {}",
                consumerSettings);
        Properties props = new Properties();
        props.putAll(consumerSettings);
        // heart beat 默认3s
        props.put("session.timeout.ms", "10000");
        // 一次性的最大拉取条数
        props.put("max.poll.records", "5");
        props.put("auto.offset.reset", "earliest");
        /* key的序列化类 */
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        /* value的序列化类 */
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        /*设置超时时间*/
        props.put("request.timeout.ms", "10500");
        return props;
    }

    @SuppressWarnings("all")
    public static void registerLagMetrics(
            AbstractFetcher kafkaFetcher, MetricGroup flinkxMetricGroup) throws Exception {
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
                    flinkxMetricGroup.addGroup(
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

    /**
     * 从kafka中获取topic信息
     *
     * @return topic list
     */
    public static List<String> getTopicListFromBroker(Properties properties) throws Exception {
        List<String> results = Lists.newArrayList();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            Map<String, List<PartitionInfo>> topics = consumer.listTopics();
            if (topics != null) {
                results.addAll(topics.keySet());
            }
        } catch (Exception e) {
            throw new Exception(e);
        }
        return results;
    }

    /**
     * 获取topic的分区数和副本数
     *
     * @return 分区数和副本数
     */
    public static Map<String, Integer> getTopicPartitionCountAndReplicas(
            Properties properties, String topic) throws Exception {
        Properties clientProp = removeExtraParam(properties);
        AdminClient client = AdminClient.create(clientProp);
        // 存放结果
        Map<String, Integer> countAndReplicas = new HashMap<>();

        DescribeTopicsResult result = client.describeTopics(Collections.singletonList(topic));
        Map<String, KafkaFuture<TopicDescription>> values = result.topicNameValues();
        KafkaFuture<TopicDescription> topicDescription = values.get(topic);
        int partitions, replicas;
        try {
            partitions = topicDescription.get().partitions().size();
            replicas = topicDescription.get().partitions().iterator().next().replicas().size();
        } catch (Exception e) {
            LOG.error("get topic partition count and replicas error:{}", e.getMessage(), e);
            throw new Exception(e);
        } finally {
            client.close();
        }
        countAndReplicas.put(KEY_PARTITIONS, partitions);
        countAndReplicas.put(KEY_REPLICAS, replicas);
        return countAndReplicas;
    }

    /**
     * 删除properties中kafka client 不需要的的参数
     *
     * @param properties properties
     * @return prop
     */
    public static Properties removeExtraParam(Properties properties) {
        Properties prop = new Properties();
        prop.putAll(properties);
        // 以下这些参数kafka client不需要
        prop.remove("enable.auto.commit");
        prop.remove("auto.commit.interval.ms");
        prop.remove("session.timeout.ms");
        prop.remove("max.poll.records");
        prop.remove("auto.offset.reset");
        prop.remove("key.deserializer");
        prop.remove("value.deserializer");
        return prop;
    }

    /**
     * 获取Topic和Group的对应关系
     *
     * @param properties
     * @param topicList
     * @return
     * @throws Exception
     */
    public static Map<String, Set<String>> getTopicGroupMap(
            Properties properties, List<String> topicList) throws Exception {

        Map<String, Set<String>> result = new HashMap<>();
        topicList.forEach(topic -> result.put(topic, new HashSet<>()));
        Properties clientProp = removeExtraParam(properties);
        AdminClient adminClient = AdminClient.create(clientProp);

        try {
            List<String> groupList = new ArrayList<>();
            // 获取所有的group信息
            ListConsumerGroupsResult listConsumerGroupsResult = adminClient.listConsumerGroups();
            KafkaFuture<Collection<ConsumerGroupListing>> valid = listConsumerGroupsResult.valid();
            Collection<ConsumerGroupListing> consumerGroupListings =
                    valid.get(5000, TimeUnit.MILLISECONDS);
            consumerGroupListings.forEach(
                    consumerGroupListing -> groupList.add(consumerGroupListing.groupId()));

            for (String groupId : groupList) {
                if (groupId.isEmpty()) {
                    continue;
                }
                adminClient
                        .listConsumerGroupOffsets(groupId)
                        .partitionsToOffsetAndMetadata()
                        .get(5000, TimeUnit.MILLISECONDS)
                        .keySet()
                        .forEach(
                                topicPartition -> {
                                    String topicName = topicPartition.topic();
                                    if (result.containsKey(topicName)
                                            && !result.get(topicName).contains(topicName)) {
                                        result.get(topicName).add(groupId);
                                    }
                                });
            }
            return result;
        } catch (Exception e) {
            throw new Exception(e);
        } finally {
            if (Objects.nonNull(adminClient)) {
                adminClient.close();
            }
        }
    }
}
