/**
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
package com.dtstack.flinkx.kafka11.reader;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.config.ReaderConfig;
import com.dtstack.flinkx.reader.DataReader;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Map;

import static com.dtstack.flinkx.kafka11.KafkaConfigKeys.*;

/**
 * company: www.dtstack.com
 * author: toutian
 * create: 2019/7/4
 */
public class Kafka11Reader extends DataReader {


    private String topic;

    private String groupId;

    private String codec;

    /**
     * true: allow blank
     */
    private boolean blankIgnore;

    private Map<String, String> consumerSettings;

    public Kafka11Reader(DataTransferConfig config, StreamExecutionEnvironment env) {
        super(config, env);
        ReaderConfig readerConfig = config.getJob().getContent().get(0).getReader();
        topic = readerConfig.getParameter().getStringVal(KEY_TOPIC);
        groupId = readerConfig.getParameter().getStringVal(KEY_GROUPID);
        codec = readerConfig.getParameter().getStringVal(KEY_CODEC, "plain");
        blankIgnore = readerConfig.getParameter().getBooleanVal(KEY_BLANK_IGNORE, false);
        consumerSettings = (Map<String, String>) readerConfig.getParameter().getVal(KEY_CONSUMER_SETTINGS);

        if (!consumerSettings.containsKey(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG)){
            throw new IllegalArgumentException(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG + " must set in consumerSettings");
        }
    }

    @Override
    public DataStream<Row> readData() {
        Kafka11InputFormat format = new Kafka11InputFormat();
        format.setTopic(topic);
        format.setGroupId(groupId);
        format.setCodec(codec);
        format.setBlankIgnore(blankIgnore);
        format.setConsumerSettings(consumerSettings);
        format.setRestoreConfig(restoreConfig);

        return createInput(format, "kafka11reader");
    }
}
