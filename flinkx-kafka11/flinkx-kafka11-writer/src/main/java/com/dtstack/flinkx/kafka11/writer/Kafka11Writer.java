/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dtstack.flinkx.kafka11.writer;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.config.WriterConfig;
import com.dtstack.flinkx.writer.DataWriter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Map;

import static com.dtstack.flinkx.kafka11.KafkaConfigKeys.*;

/**
 * company: www.dtstack.com
 * author: toutian
 * create: 2019/7/4
 */
public class Kafka11Writer extends DataWriter {

    private String timezone;

    private String topic;

    private Map<String, String> producerSettings;

    public Kafka11Writer(DataTransferConfig config) {
        super(config);
        WriterConfig writerConfig = config.getJob().getContent().get(0).getWriter();
        timezone = writerConfig.getParameter().getStringVal(KEY_TIMEZONE);
        topic = writerConfig.getParameter().getStringVal(KEY_TOPIC);
        producerSettings = (Map<String, String>) writerConfig.getParameter().getVal(KEY_PRODUCER_SETTINGS);

        if (!producerSettings.containsKey(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG)){
            throw new IllegalArgumentException(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG + " must set in producerSettings");
        }
    }

    @Override
    public DataStreamSink<?> writeData(DataStream<Row> dataSet) {
        Kafka11OutputFormat format = new Kafka11OutputFormat();
        format.setTimezone(timezone);
        format.setTopic(topic);
        format.setProducerSettings(producerSettings);
        format.setRestoreConfig(restoreConfig);

        return createOutput(dataSet, format, "kafka11writer");
    }
}
