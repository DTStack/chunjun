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

package com.dtstack.chunjun.connector.kafka.sink;

import com.dtstack.chunjun.restore.FormatState;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaException;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.table.data.RowData;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class KafkaProducer extends FlinkKafkaProducer<RowData> {

    protected static final String LOCATION_STATE_NAME = "data-sync-location-states";
    private static final Logger LOG = LoggerFactory.getLogger(KafkaProducer.class);
    protected transient ListState<FormatState> unionOffsetStates;
    protected Map<Integer, FormatState> formatStateMap;
    private final KafkaSerializationSchema<RowData> serializationSchema;
    private final Properties producerConfig;

    public KafkaProducer(
            String defaultTopic,
            KafkaSerializationSchema<RowData> serializationSchema,
            Properties producerConfig,
            Semantic semantic,
            int kafkaProducersPoolSize) {
        super(defaultTopic, serializationSchema, producerConfig, semantic, kafkaProducersPoolSize);
        this.serializationSchema = serializationSchema;
        this.producerConfig = producerConfig;
    }

    @Override
    public void open(Configuration configuration) throws Exception {
        RuntimeContext runtimeContext = getRuntimeContext();
        ((DynamicKafkaSerializationSchema) serializationSchema).setRuntimeContext(runtimeContext);
        ((DynamicKafkaSerializationSchema) serializationSchema).setProducerConfig(producerConfig);
        if (formatStateMap != null) {
            ((DynamicKafkaSerializationSchema) serializationSchema)
                    .setFormatState(formatStateMap.get(runtimeContext.getIndexOfThisSubtask()));
        }
        super.open(configuration);
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        super.snapshotState(context);
        FormatState formatState =
                ((DynamicKafkaSerializationSchema) serializationSchema).getFormatState();
        if (formatState != null) {
            LOG.info("OutputFormat format state:{}", formatState);
            unionOffsetStates.clear();
            unionOffsetStates.add(formatState);
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        super.initializeState(context);
        LOG.info("Start initialize output format state");
        OperatorStateStore stateStore = context.getOperatorStateStore();
        unionOffsetStates =
                stateStore.getUnionListState(
                        new ListStateDescriptor<>(
                                LOCATION_STATE_NAME,
                                TypeInformation.of(new TypeHint<FormatState>() {})));

        LOG.info("Is restored:{}", context.isRestored());
        if (context.isRestored()) {
            formatStateMap = new HashMap<>(16);
            for (FormatState formatState : unionOffsetStates.get()) {
                formatStateMap.put(formatState.getNumOfSubTask(), formatState);
                LOG.info("Output format state into:{}", formatState);
            }
        }
        LOG.info("End initialize output format state");
    }

    @Override
    public void close() throws FlinkKafkaException {
        super.close();
        ((DynamicKafkaSerializationSchema) serializationSchema).close();
    }
}
