/*
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
package com.dtstack.flinkx.kafka09.format;

import com.dtstack.flinkx.kafka09.client.Kafka09Consumer;
import com.dtstack.flinkx.kafkabase.KafkaInputSplit;
import com.dtstack.flinkx.kafkabase.enums.KafkaVersion;
import com.dtstack.flinkx.kafkabase.format.KafkaBaseInputFormat;
import com.dtstack.flinkx.kafkabase.util.KafkaUtil;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.apache.flink.core.io.InputSplit;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * @company: www.dtstack.com
 * @author: toutian
 * @create: 2019/7/5
 */
public class Kafka09InputFormat extends KafkaBaseInputFormat {

    private transient ConsumerConnector consumerConnector;

    @Override
    public void openInputFormat() throws IOException {
        super.openInputFormat();
        Properties props = KafkaUtil.geneConsumerProp(consumerSettings, mode);
        consumerConnector = kafka.consumer.Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
    }

    @Override
    protected void openInternal(InputSplit inputSplit) {
        Map<String, Integer> topicCountMap = Collections.singletonMap(topic, 1);
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumerConnector.createMessageStreams(topicCountMap);

        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
        for (final KafkaStream<byte[], byte[]> stream : streams) {
            consumer = new Kafka09Consumer(stream);
        }
        consumer.createClient(topic, groupId, this, (KafkaInputSplit)inputSplit).execute();
        running = true;
    }

    @Override
    protected void closeInternal() {
        if (running) {
            consumerConnector.commitOffsets(true);
            consumerConnector.shutdown();
            consumer.close();
            running = false;
            LOG.warn("input kafka release.");
        }
    }

    @Override
    public KafkaVersion getKafkaVersion() {
        return KafkaVersion.kafka09;
    }
}
