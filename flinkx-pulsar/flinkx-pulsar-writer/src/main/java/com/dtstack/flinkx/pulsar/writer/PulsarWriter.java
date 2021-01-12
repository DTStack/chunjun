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
package com.dtstack.flinkx.pulsar.writer;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.pulsar.format.PulsarOutputFormatBuilder;
import com.dtstack.flinkx.writer.BaseDataWriter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.types.Row;

import java.util.List;
import java.util.Map;

import static com.dtstack.flinkx.pulsar.format.Constants.*;


/**
 * @author: pierre
 * @create: 2020/3/21
 */
public class PulsarWriter extends BaseDataWriter {
    protected String topic;
    protected String token;
    protected String pulsarServiceUrl;
    protected List<String> tableFields;
    protected Map<String, Object> producerSettings;

    @SuppressWarnings("unchecked")
    public PulsarWriter(DataTransferConfig config){
        super(config);
        topic = config.getJob().getContent().get(0).getWriter().getParameter().getStringVal(KEY_TOPIC);
        token = config.getJob().getContent().get(0).getWriter().getParameter().getStringVal(KEY_TOKEN);
        pulsarServiceUrl = config.getJob().getContent().get(0).getWriter().getParameter().getStringVal(KEY_PULSAR_SERVICE_URL);
        producerSettings = (Map<String, Object>) config.getJob().getContent().get(0).getWriter().getParameter().getVal(KEY_PRODUCER_SETTINGS);
        tableFields = (List<String>)config.getJob().getContent().get(0).getWriter().getParameter().getVal(KEY_TABLE_FIELDS);
    }

    @Override
    public DataStreamSink<?> writeData(DataStream<Row> dataSet) {
        PulsarOutputFormatBuilder builder = new PulsarOutputFormatBuilder();
        builder.setTopic(topic);
        builder.setPulsarServiceUrl(pulsarServiceUrl);
        builder.setProducerSettings(producerSettings);
        builder.setToken(token);
        return createOutput(dataSet, builder.finish());
    }
}
