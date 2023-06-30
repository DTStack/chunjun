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

import com.dtstack.chunjun.connector.kafka.conf.KafkaConfig;
import com.dtstack.chunjun.connector.kafka.source.DynamicKafkaDeserializationSchema;
import com.dtstack.chunjun.constants.Metrics;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.util.JsonUtil;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.nio.charset.StandardCharsets;

/**
 * Date: 2021/03/04 Company: www.dtstack.com
 *
 * @author tudou
 */
public class RowDeserializationSchema extends DynamicKafkaDeserializationSchema {

    private static final long serialVersionUID = 1L;
    /** kafka converter */
    private final AbstractRowConverter<ConsumerRecord<byte[], byte[]>, Object, byte[], String>
            converter;
    /** kafka conf */
    private final KafkaConfig kafkaConfig;

    public RowDeserializationSchema(
            KafkaConfig kafkaConfig,
            AbstractRowConverter<ConsumerRecord<byte[], byte[]>, Object, byte[], String>
                    converter) {
        super(1, null, null, null, null, false, null, null, false);
        this.kafkaConfig = kafkaConfig;
        this.converter = converter;
    }

    @Override
    public void open(DeserializationSchema.InitializationContext context) {
        beforeOpen();
        LOG.info(
                "[{}] open successfully, \ninputSplit = {}, \n[{}]: \n{} ",
                this.getClass().getSimpleName(),
                "see other log",
                kafkaConfig.getClass().getSimpleName(),
                JsonUtil.toPrintJson(kafkaConfig));
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<RowData> collector) {
        try {
            beforeDeserialize(record);
            collector.collect(converter.toInternal(record));
        } catch (Exception e) {
            String data = null;
            if (record.value() != null) {
                data = new String(record.value(), StandardCharsets.UTF_8);
            }
            long globalErrors = accumulatorCollector.getAccumulatorValue(Metrics.NUM_ERRORS, false);
            dirtyManager.collect(
                    new String(record.value(), StandardCharsets.UTF_8), e, null, globalErrors);
        }
    }
}
