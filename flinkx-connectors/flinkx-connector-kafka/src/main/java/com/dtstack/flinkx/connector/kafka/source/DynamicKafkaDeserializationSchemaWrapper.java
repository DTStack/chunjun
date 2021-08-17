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

import com.dtstack.flinkx.util.ReflectionUtils;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.connectors.kafka.internals.AbstractFetcher;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaConsumerThread;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartitionState;
import org.apache.flink.table.data.RowData;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.internals.SubscriptionState;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.lang.reflect.Field;
import java.util.List;

import static com.dtstack.flinkx.metrics.MetricConstant.DT_PARTITION_GROUP;
import static com.dtstack.flinkx.metrics.MetricConstant.DT_TOPIC_GROUP;
import static com.dtstack.flinkx.metrics.MetricConstant.DT_TOPIC_PARTITION_LAG_GAUGE;

/**
 * @author chuixue
 * @create 2021-05-07 15:17
 * @description
 */
public class DynamicKafkaDeserializationSchemaWrapper extends DynamicKafkaDeserializationSchema {

    protected static final Logger LOG =
            LoggerFactory.getLogger(DynamicKafkaDeserializationSchemaWrapper.class);

    private static final long serialVersionUID = 2L;

    private Calculate calculate;

    protected DynamicKafkaDeserializationSchemaWrapper(
            int physicalArity,
            @Nullable DeserializationSchema<RowData> keyDeserialization,
            int[] keyProjection,
            DeserializationSchema<RowData> valueDeserialization,
            int[] valueProjection,
            boolean hasMetadata,
            MetadataConverter[] metadataConverters,
            TypeInformation<RowData> producedTypeInfo,
            boolean upsertMode,
            Calculate calculate) {
        super(
                physicalArity,
                keyDeserialization,
                keyProjection,
                valueDeserialization,
                valueProjection,
                hasMetadata,
                metadataConverters,
                producedTypeInfo,
                upsertMode);
        this.calculate = calculate;
    }

    protected void registerPtMetric(AbstractFetcher<RowData, ?> fetcher) throws Exception {
        Field consumerThreadField = ReflectionUtils.getDeclaredField(fetcher, "consumerThread");
        consumerThreadField.setAccessible(true);
        KafkaConsumerThread consumerThread = (KafkaConsumerThread) consumerThreadField.get(fetcher);

        Field hasAssignedPartitionsField =
                consumerThread.getClass().getDeclaredField("hasAssignedPartitions");
        hasAssignedPartitionsField.setAccessible(true);

        // get subtask unassigned kafka topic partition
        Field subscribedPartitionStatesField =
                ReflectionUtils.getDeclaredField(fetcher, "subscribedPartitionStates");
        subscribedPartitionStatesField.setAccessible(true);
        List<KafkaTopicPartitionState<?, KafkaTopicPartition>> subscribedPartitionStates =
                (List<KafkaTopicPartitionState<?, KafkaTopicPartition>>)
                        subscribedPartitionStatesField.get(fetcher);

        // init partition lag metric
        for (KafkaTopicPartitionState<?, KafkaTopicPartition> kafkaTopicPartitionState :
                subscribedPartitionStates) {
            KafkaTopicPartition kafkaTopicPartition =
                    kafkaTopicPartitionState.getKafkaTopicPartition();
            MetricGroup topicMetricGroup =
                    getRuntimeContext()
                            .getMetricGroup()
                            .addGroup(DT_TOPIC_GROUP, kafkaTopicPartition.getTopic());

            MetricGroup metricGroup =
                    topicMetricGroup.addGroup(
                            DT_PARTITION_GROUP, kafkaTopicPartition.getPartition() + "");
            metricGroup.gauge(
                    DT_TOPIC_PARTITION_LAG_GAUGE,
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
                                    LOG.error(e.getMessage());
                                }
                                return -1L;
                            } else {
                                return calculate.calc(subscriptionState, topicPartition);
                            }
                        }
                    });
        }
    }

    public void setFetcher(AbstractFetcher<RowData, ?> fetcher) {
        try {
            registerPtMetric(fetcher);
        } catch (Exception e) {
            LOG.error("register topic partition metric error.", e);
        }
    }
}
