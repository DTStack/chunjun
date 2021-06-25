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

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.data.RowData;

import com.dtstack.flinkx.connector.kafka.sink.DynamicKafkaSerializationSchema;
import com.dtstack.flinkx.converter.AbstractRowConverter;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

/**
 * Date: 2021/03/04
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public class RowSerializationSchema extends DynamicKafkaSerializationSchema {

    protected final Logger LOG = LoggerFactory.getLogger(getClass());

    private static final long serialVersionUID = 1L;
    /** kafka converter */
    private final AbstractRowConverter<String, Object, ProducerRecord, String> converter;

    public RowSerializationSchema(String topic, AbstractRowConverter converter) {
        super(
                topic,
                null,
                null,
                null,
                null,
                null,
                false,
                null,
                false);
        this.converter = converter;
    }

    @Override
    public void open(SerializationSchema.InitializationContext context) throws Exception {

    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(RowData element, @Nullable Long timestamp) {
        try {
            dtNumRecordsOut.inc();
            return converter.toExternal(element, null);
        } catch (Exception e) {
            // todo kafka比较特殊，这里直接记录脏数据。
            LOG.error(e.getMessage());
        }
        return null;
    }
}
