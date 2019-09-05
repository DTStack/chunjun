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
package com.dtstack.flinkx.kafka09.reader;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.config.ReaderConfig;
import com.dtstack.flinkx.reader.DataReader;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

import java.util.Map;

import static com.dtstack.flinkx.kafka09.KafkaConfigKeys.*;

/**
 * company: www.dtstack.com
 * author: toutian
 * create: 2019/7/4
 */
public class Kafka09Reader extends DataReader {

    private String topic;

    private String codec;

    private String encoding;

    private Map<String, String> consumerSettings;

    public Kafka09Reader(DataTransferConfig config, StreamExecutionEnvironment env) {
        super(config, env);
        ReaderConfig readerConfig = config.getJob().getContent().get(0).getReader();
        topic = readerConfig.getParameter().getStringVal(KEY_TOPIC);
        codec = readerConfig.getParameter().getStringVal(KEY_CODEC, "plain");
        consumerSettings = (Map<String, String>) readerConfig.getParameter().getVal(KEY_CONSUMER_SETTINGS);
        encoding = readerConfig.getParameter().getStringVal(KEY_ENCODING, "utf-8");
    }

    @Override
    public DataStream<Row> readData() {
        Kafka09InputFormat format = new Kafka09InputFormat();
        format.setTopic(topic);
        format.setCodec(codec);
        format.setConsumerSettings(consumerSettings);
        format.setEncoding(encoding);
        format.setRestoreConfig(restoreConfig);

        return createInput(format, "kafka09reader");
    }
}
