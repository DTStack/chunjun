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

import com.dtstack.flinkx.restore.FormatState;
import com.dtstack.flinkx.util.ReflectionUtils;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.internals.AbstractFetcher;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.table.data.RowData;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * @author chuixue
 * @create 2021-05-07 14:46
 * @description
 */
public class KafkaConsumer extends RichParallelSourceFunction<RowData>
        implements CheckpointListener, ResultTypeQueryable<RowData>, CheckpointedFunction {

    protected static final Logger LOG = LoggerFactory.getLogger(KafkaConsumer.class);
    private static final long serialVersionUID = -8815725381567887426L;

    private FlinkKafkaConsumerBase<RowData> flinkKafkaConsumer;
    private final KafkaDeserializationSchema<RowData> deserializationSchema;
    private Properties props;
    private transient ListState<FormatState> unionOffsetStates;
    private static final String LOCATION_STATE_NAME = "data-sync-location-states";
    private Map<Integer, FormatState> formatStateMap;

    public KafkaConsumer(
            List<String> topics,
            KafkaDeserializationSchema<RowData> deserializer,
            Properties props) {
        flinkKafkaConsumer = new FlinkKafkaConsumer<>(topics, deserializer, props);
        this.deserializationSchema = deserializer;
        this.props = props;
    }

    public KafkaConsumer(
            Pattern subscriptionPattern,
            KafkaDeserializationSchema<RowData> deserializer,
            Properties props) {
        flinkKafkaConsumer = new FlinkKafkaConsumer<>(subscriptionPattern, deserializer, props);
        this.deserializationSchema = deserializer;
        this.props = props;
    }

    @Override
    public void setRuntimeContext(RuntimeContext t) {
        super.setRuntimeContext(t);
        flinkKafkaConsumer.setRuntimeContext(t);
    }

    @Override
    public void open(Configuration configuration) throws Exception {
        ((DynamicKafkaDeserializationSchemaWrapper) deserializationSchema)
                .setRuntimeContext(getRuntimeContext());
        ((DynamicKafkaDeserializationSchemaWrapper) deserializationSchema).setConsumerConfig(props);
        if (formatStateMap != null) {
            ((DynamicKafkaDeserializationSchemaWrapper) deserializationSchema)
                    .setFormatState(
                            formatStateMap.get(getRuntimeContext().getIndexOfThisSubtask()));
        }
        flinkKafkaConsumer.open(configuration);
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        flinkKafkaConsumer.snapshotState(context);
        FormatState formatState =
                ((DynamicKafkaDeserializationSchemaWrapper) deserializationSchema).getFormatState();
        if (formatState != null) {
            LOG.info("InputFormat format state:{}", formatState);
            unionOffsetStates.clear();
            unionOffsetStates.add(formatState);
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        flinkKafkaConsumer.initializeState(context);
        OperatorStateStore stateStore = context.getOperatorStateStore();
        LOG.info("Start initialize input format state, is restored:{}", context.isRestored());
        unionOffsetStates =
                stateStore.getUnionListState(
                        new ListStateDescriptor<>(
                                LOCATION_STATE_NAME,
                                TypeInformation.of(new TypeHint<FormatState>() {})));
        if (context.isRestored()) {
            formatStateMap = new HashMap<>(16);
            for (FormatState formatState : unionOffsetStates.get()) {
                formatStateMap.put(formatState.getNumOfSubTask(), formatState);
                LOG.info("Input format state into:{}", formatState);
            }
        }
        LOG.info("End initialize input format state");
    }

    @Override
    public void close() throws Exception {
        flinkKafkaConsumer.close();
        ((DynamicKafkaDeserializationSchemaWrapper) deserializationSchema).close();
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        flinkKafkaConsumer.notifyCheckpointComplete(checkpointId);
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return flinkKafkaConsumer.getProducedType();
    }

    @Override
    public void run(SourceContext<RowData> ctx) throws Exception {
        flinkKafkaConsumer.run(ctx);
        // get kafkaFetcher from consumer
        Field field = ReflectionUtils.getDeclaredField(flinkKafkaConsumer, "kafkaFetcher");
        field.setAccessible(true);
        AbstractFetcher kafkaFetcher = (AbstractFetcher) field.get(flinkKafkaConsumer);
        ((DynamicKafkaDeserializationSchemaWrapper) deserializationSchema).setFetcher(kafkaFetcher);
    }

    @Override
    public void cancel() {
        flinkKafkaConsumer.cancel();
    }

    public FlinkKafkaConsumerBase<RowData> setStartFromEarliest() {
        return flinkKafkaConsumer.setStartFromEarliest();
    }

    public FlinkKafkaConsumerBase<RowData> setStartFromLatest() {
        return flinkKafkaConsumer.setStartFromLatest();
    }

    public FlinkKafkaConsumerBase<RowData> setStartFromGroupOffsets() {
        return flinkKafkaConsumer.setStartFromGroupOffsets();
    }

    public FlinkKafkaConsumerBase<RowData> setStartFromSpecificOffsets(
            Map<KafkaTopicPartition, Long> specificStartupOffsets) {
        return flinkKafkaConsumer.setStartFromSpecificOffsets(specificStartupOffsets);
    }

    public FlinkKafkaConsumerBase<RowData> setStartFromTimestamp(long startupOffsetsTimestamp) {
        return flinkKafkaConsumer.setStartFromTimestamp(startupOffsetsTimestamp);
    }

    public FlinkKafkaConsumerBase<RowData> setCommitOffsetsOnCheckpoints(
            boolean commitOnCheckpoints) {
        return flinkKafkaConsumer.setCommitOffsetsOnCheckpoints(commitOnCheckpoints);
    }

    public FlinkKafkaConsumerBase<RowData> assignTimestampsAndWatermarks(
            WatermarkStrategy<RowData> watermarkStrategy) {
        return flinkKafkaConsumer.assignTimestampsAndWatermarks(watermarkStrategy);
    }
}
