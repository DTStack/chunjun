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
package com.dtstack.flinkx.connector.kafka.serialization;

import com.dtstack.flinkx.connector.kafka.conf.KafkaConf;
import com.dtstack.flinkx.connector.kafka.source.Calculate;
import com.dtstack.flinkx.connector.kafka.source.DynamicKafkaDeserializationSchemaWrapper;
import com.dtstack.flinkx.converter.AbstractRowConverter;
import com.dtstack.flinkx.util.JsonUtil;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;

/**
 * Date: 2021/03/04 Company: www.dtstack.com
 *
 * @author tudou
 */
public class RowDeserializationSchema extends DynamicKafkaDeserializationSchemaWrapper {

    private static final long serialVersionUID = 1L;
    /** kafka converter */
    private final AbstractRowConverter converter;
    /** kafka conf */
    private final KafkaConf kafkaConf;

    public RowDeserializationSchema(
            KafkaConf kafkaConf, AbstractRowConverter converter, Calculate calculate) {
        super(1, null, null, null, null, false, null, null, false, calculate);
        this.kafkaConf = kafkaConf;
        this.converter = converter;
    }

    @Override
    public void open(DeserializationSchema.InitializationContext context) {
        beforeOpen();
        LOG.info(
                "[{}] open successfully, \ninputSplit = {}, \n[{}]: \n{} ",
                this.getClass().getSimpleName(),
                "see other log",
                kafkaConf.getClass().getSimpleName(),
                JsonUtil.toPrintJson(kafkaConf));
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<RowData> collector)
            throws UnsupportedEncodingException {
        try {
            beforeDeserialize(record);
            collector.collect(
                    converter.toInternal(new String(record.value(), StandardCharsets.UTF_8)));
        } catch (Exception e) {
            // todo kafka 比较特殊这里直接对接脏数据即可
            dirtyDataCounter(record, e);
        }
    }
}
