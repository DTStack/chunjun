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
package com.dtstack.flinkx.pulsar.format;

import com.dtstack.flinkx.decoder.JsonDecoder;
import com.dtstack.flinkx.exception.WriteRecordException;
import com.dtstack.flinkx.outputformat.BaseRichOutputFormat;
import com.dtstack.flinkx.util.ExceptionUtil;
import com.dtstack.flinkx.util.MapUtil;
import org.apache.flink.types.Row;
import org.apache.flink.util.StringUtils;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * @author: pierre
 * @create: 2020/3/21
 */
public class PulsarOutputFormat extends BaseRichOutputFormat {

    private transient Producer producer;

    protected String topic;
    protected String pulsarServiceUrl;
    protected String token;
    protected Map<String, Object> producerSettings;

    protected List<String> tableFields;
    protected static JsonDecoder jsonDecoder = new JsonDecoder();

    @Override
    protected void openInternal(int taskNumber, int numTasks) throws IOException {
        PulsarClient client;

        if (null != token) {
            client = PulsarClient.builder()
                    .serviceUrl(pulsarServiceUrl)
                    .authentication(AuthenticationFactory.token(token))
                    .build();
        } else {
            client = PulsarClient.builder()
                    .serviceUrl(pulsarServiceUrl)
                    .build();
        }
        // pulsar-client 2.4.0 loadConf有bug
        producer = client.newProducer(Schema.STRING)
                .topic(topic)
                .loadConf(producerSettings)
                .create();
    }

    @Override
    protected void writeSingleRecordInternal(Row row) throws WriteRecordException {
        // copy from kafka-writer
        try {
            Map<String, Object> map;
            int arity = row.getArity();
            if (tableFields != null && tableFields.size() >= arity) {
                map = new LinkedHashMap<>((arity << 2) / 3);
                for (int i = 0; i < arity; i++) {
                    map.put(tableFields.get(i), StringUtils.arrayAwareToString(row.getField(i)));
                }
            } else {
                if (arity == 1) {
                    Object obj = row.getField(0);
                    if (obj instanceof Map) {
                        map = (Map<String, Object>) obj;
                    } else if (obj instanceof String) {
                        map = jsonDecoder.decode(obj.toString());
                    } else {
                        map = Collections.singletonMap("message", row.toString());
                    }
                } else {
                    map = Collections.singletonMap("message", row.toString());
                }
            }
            emit(map);
        } catch (Throwable e) {
            LOG.error("pulsar writeSingleRecordInternal error:{}", ExceptionUtil.getErrorMessage(e));
            throw new WriteRecordException(e.getMessage(), e);
        }
    }

    protected void emit(Map event) throws IOException {
        producer.send(MapUtil.writeValueAsString(event));
    }

    @Override
    protected void writeMultipleRecordsInternal() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void closeInternal() throws IOException {
        LOG.warn("pulsar output closeInternal.");
        producer.close();
    }
}
