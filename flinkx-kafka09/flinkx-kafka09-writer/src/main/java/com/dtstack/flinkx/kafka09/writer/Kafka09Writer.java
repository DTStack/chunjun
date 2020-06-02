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
package com.dtstack.flinkx.kafka09.writer;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.config.WriterConfig;
import com.dtstack.flinkx.kafkabase.KafkaConfigKeys;
import com.dtstack.flinkx.kafkabase.writer.KafkaBaseWriter;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.types.Row;

import java.nio.charset.StandardCharsets;

/**
 * @company: www.dtstack.com
 * @author: toutian
 * @create: 2019/7/4
 */
public class Kafka09Writer extends KafkaBaseWriter {

    private String encoding;
    private String brokerList;

    public Kafka09Writer(DataTransferConfig config) {
        super(config);
        WriterConfig writerConfig = config.getJob().getContent().get(0).getWriter();
        encoding = writerConfig.getParameter().getStringVal(KafkaConfigKeys.KEY_ENCODING, StandardCharsets.UTF_8.name());
        brokerList = writerConfig.getParameter().getStringVal(KafkaConfigKeys.KEY_BROKER_LIST);
        if (StringUtils.isBlank(brokerList)) {
            throw new RuntimeException("brokerList can not be empty!");
        }
    }

    @Override
    public DataStreamSink<?> writeData(DataStream<Row> dataSet) {
        Kafka09OutputFormat format = new Kafka09OutputFormat();
        format.setTimezone(timezone);
        format.setEncoding(encoding);
        format.setTopic(topic);
        format.setTableFields(tableFields);
        format.setBrokerList(brokerList);
        format.setProducerSettings(producerSettings);
        format.setRestoreConfig(restoreConfig);

        return createOutput(dataSet, format);
    }
}
