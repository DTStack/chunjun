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
package com.dtstack.chunjun.connector.kafka.serialization;

import com.dtstack.chunjun.cdc.DdlRowData;
import com.dtstack.chunjun.connector.kafka.conf.KafkaConfig;
import com.dtstack.chunjun.connector.kafka.sink.DynamicKafkaSerializationSchema;
import com.dtstack.chunjun.constants.CDCConstantValue;
import com.dtstack.chunjun.constants.Metrics;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.element.AbstractBaseColumn;
import com.dtstack.chunjun.element.ColumnRowData;
import com.dtstack.chunjun.element.column.NullColumn;
import com.dtstack.chunjun.util.GsonUtil;
import com.dtstack.chunjun.util.JsonUtil;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.table.data.RowData;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

/**
 * Date: 2021/03/04 Company: www.dtstack.com
 *
 * @author tudou
 */
public class RowSerializationSchema extends DynamicKafkaSerializationSchema {

    private static final long serialVersionUID = 1L;
    protected final Logger LOG = LoggerFactory.getLogger(getClass());
    /** kafka key converter */
    private final AbstractRowConverter<String, Object, byte[], String> keyConverter;
    /** kafka value converter */
    private final AbstractRowConverter<String, Object, byte[], String> valueConverter;
    /** kafka converter */
    private final KafkaConfig kafkaConfig;

    private String currentTopic;

    public RowSerializationSchema(
            KafkaConfig kafkaConfig,
            @Nullable FlinkKafkaPartitioner<RowData> partitioner,
            AbstractRowConverter keyConverter,
            AbstractRowConverter valueConverter) {
        super(kafkaConfig.getTopic(), partitioner, null, null, null, null, false, null, false);
        this.keyConverter = keyConverter;
        this.valueConverter = valueConverter;
        this.kafkaConfig = kafkaConfig;
        this.currentTopic = kafkaConfig.getTopic();
    }

    @Override
    public void open(SerializationSchema.InitializationContext context) throws Exception {
        beforeOpen();
        LOG.info(
                "[{}] open successfully, \ncheckpointMode = {}, \ncheckpointEnabled = {}, \nflushIntervalMills = {}, \nbatchSize = {}, \n[{}]: \n{} ",
                this.getClass().getSimpleName(),
                checkpointMode,
                checkpointEnabled,
                0,
                1,
                kafkaConfig.getClass().getSimpleName(),
                JsonUtil.toPrintJson(kafkaConfig));
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(RowData element, @Nullable Long timestamp) {
        try {
            beforeSerialize(1, element);
            byte[] keySerialized = null;
            if (keyConverter != null) {
                keySerialized = keyConverter.toExternal(element, null);
            }
            byte[] valueSerialized = valueConverter.toExternal(element, null);
            return new ProducerRecord<>(
                    this.currentTopic,
                    extractPartition(element, keySerialized, null),
                    null,
                    valueSerialized);
        } catch (Exception e) {
            long globalErrors = accumulatorCollector.getAccumulatorValue(Metrics.NUM_ERRORS, false);
            dirtyManager.collect(element, e, null, globalErrors);
        }
        return null;
    }

    @Override
    public String getTargetTopic(RowData element) {
        if (CollectionUtils.isNotEmpty(kafkaConfig.getTopics())
                && kafkaConfig.getTopics().size() > 1) {
            if (element instanceof ColumnRowData) {
                ColumnRowData columnRowData = (ColumnRowData) element;
                if (CollectionUtils.isNotEmpty(columnRowData.getExtHeader())
                        && columnRowData
                                .getExtHeader()
                                .contains("org/apache/flink/streaming/connectors/kafka/table")) {
                    AbstractBaseColumn database = columnRowData.getField(CDCConstantValue.DATABASE);
                    AbstractBaseColumn schema = columnRowData.getField(CDCConstantValue.SCHEMA);
                    AbstractBaseColumn table = columnRowData.getField(CDCConstantValue.TABLE);
                    columnRowData.removeExtHeaderInfo();
                    if (database != null && !(database instanceof NullColumn)) {
                        currentTopic = database.asString();
                        return currentTopic;
                    }
                    if (schema != null && !(schema instanceof NullColumn)) {
                        currentTopic = schema.asString();
                        return currentTopic;
                    }
                    if (table != null && !(table instanceof NullColumn)) {
                        currentTopic = table.asString();
                        return currentTopic;
                    }
                }
            } else if (element instanceof DdlRowData) {
                DdlRowData ddlRowData = (DdlRowData) element;
                String table = ddlRowData.getTableIdentifier().tableIdentifier("");
                if (StringUtils.isNotEmpty(table)) {
                    currentTopic = table;
                    return currentTopic;
                }
            }
            throw new RuntimeException(
                    "getTopic failed when kafkaConf.getTopics() is not empty and have more one topic, topics { "
                            + GsonUtil.GSON.toJson(kafkaConfig.getTopics())
                            + " } and data is "
                            + element);
        }

        return super.getTargetTopic(element);
    }
}
