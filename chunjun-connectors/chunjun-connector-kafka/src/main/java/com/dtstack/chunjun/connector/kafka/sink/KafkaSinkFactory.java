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
package com.dtstack.chunjun.connector.kafka.sink;

import com.dtstack.chunjun.config.SyncConfig;
import com.dtstack.chunjun.connector.kafka.adapter.StartupModeAdapter;
import com.dtstack.chunjun.connector.kafka.conf.KafkaConfig;
import com.dtstack.chunjun.connector.kafka.converter.KafkaRawTypeMapping;
import com.dtstack.chunjun.connector.kafka.converter.KafkaSyncConverter;
import com.dtstack.chunjun.connector.kafka.enums.StartupMode;
import com.dtstack.chunjun.connector.kafka.partitioner.CustomerFlinkPartition;
import com.dtstack.chunjun.connector.kafka.serialization.RowSerializationSchema;
import com.dtstack.chunjun.converter.RawTypeMapper;
import com.dtstack.chunjun.sink.SinkFactory;
import com.dtstack.chunjun.util.GsonUtil;
import com.dtstack.chunjun.util.TableUtil;

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.Preconditions;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.Properties;

public class KafkaSinkFactory extends SinkFactory {

    protected KafkaConfig kafkaConfig;

    public KafkaSinkFactory(SyncConfig config) {
        super(config);
        Gson gson =
                new GsonBuilder()
                        .registerTypeAdapter(StartupMode.class, new StartupModeAdapter())
                        .create();
        GsonUtil.setTypeAdapter(gson);
        kafkaConfig =
                gson.fromJson(gson.toJson(config.getWriter().getParameter()), KafkaConfig.class);
        kafkaConfig.setColumn(config.getWriter().getFieldList());
        if (CollectionUtils.isNotEmpty(kafkaConfig.getTopics())
                && StringUtils.isEmpty(kafkaConfig.getTopic())) {
            kafkaConfig.setTopic(kafkaConfig.getTopics().get(0));
        }
        super.initCommonConf(kafkaConfig);
    }

    @Override
    protected DataStreamSink<RowData> createOutput(
            DataStream<RowData> dataSet, OutputFormat<RowData> outputFormat) {
        return createOutput(dataSet, outputFormat, this.getClass().getSimpleName().toLowerCase());
    }

    @Override
    protected DataStreamSink<RowData> createOutput(
            DataStream<RowData> dataSet, OutputFormat<RowData> outputFormat, String sinkName) {
        Preconditions.checkNotNull(dataSet);
        Preconditions.checkNotNull(sinkName);

        Properties props = new Properties();
        props.putAll(kafkaConfig.getProducerSettings());

        if (kafkaConfig.isDataCompelOrder()) {
            Preconditions.checkState(
                    kafkaConfig.getParallelism() <= 1,
                    "when kafka sink dataCompelOrder set true , Parallelism must 1.");
        }

        RowSerializationSchema rowSerializationSchema;
        RowType rowType =
                TableUtil.createRowType(kafkaConfig.getColumn(), KafkaRawTypeMapping::apply);
        if (!CollectionUtil.isNullOrEmpty(kafkaConfig.getPartitionAssignColumns())) {
            Preconditions.checkState(
                    !CollectionUtil.isNullOrEmpty(kafkaConfig.getTableFields()),
                    "when kafka sink set partitionAssignColumns , tableFields must set.");
            for (String field : kafkaConfig.getPartitionAssignColumns()) {
                Preconditions.checkState(
                        kafkaConfig.getTableFields().contains(field),
                        "["
                                + field
                                + "] field in partitionAssignColumns , but not in tableFields:["
                                + kafkaConfig.getTableFields()
                                + "]");
            }
            rowSerializationSchema =
                    new RowSerializationSchema(
                            kafkaConfig,
                            new CustomerFlinkPartition<>(),
                            new KafkaSyncConverter(
                                    rowType, kafkaConfig, kafkaConfig.getPartitionAssignColumns()),
                            new KafkaSyncConverter(rowType, kafkaConfig));
        } else {
            rowSerializationSchema =
                    new RowSerializationSchema(
                            kafkaConfig,
                            new CustomerFlinkPartition<>(),
                            null,
                            new KafkaSyncConverter(rowType, kafkaConfig));
        }

        KafkaProducer kafkaProducer =
                new KafkaProducer(
                        kafkaConfig.getTopic(),
                        rowSerializationSchema,
                        props,
                        FlinkKafkaProducer.Semantic.AT_LEAST_ONCE,
                        FlinkKafkaProducer.DEFAULT_KAFKA_PRODUCERS_POOL_SIZE);

        DataStreamSink<RowData> dataStreamSink = dataSet.addSink(kafkaProducer);
        dataStreamSink.name(sinkName);

        return dataStreamSink;
    }

    @Override
    public DataStreamSink<RowData> createSink(DataStream<RowData> dataSet) {
        if (!useAbstractBaseColumn) {
            throw new UnsupportedOperationException("kafka not support transform");
        }
        return createOutput(dataSet, null);
    }

    @Override
    public RawTypeMapper getRawTypeMapper() {
        return KafkaRawTypeMapping::apply;
    }
}
