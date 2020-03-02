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
package com.dtstack.flinkx.kafka09.writer;

import com.dtstack.flinkx.kafkabase.Formatter;
import com.dtstack.flinkx.kafkabase.writer.KafkaBaseOutputFormat;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.flink.configuration.Configuration;

import java.io.IOException;
import java.util.Map;

/**
 * @company: www.dtstack.com
 * @author: toutian
 * @create: 2019/7/5
 */
public class Kafka09OutputFormat extends KafkaBaseOutputFormat {

    private String encoding;
    private String brokerList;
    private transient Producer<String, byte[]> producer;

    @Override
    public void configure(Configuration parameters) {
        props.put("key.serializer.class", "kafka.serializer.StringEncoder");
        props.put("value.serializer.class", "kafka.serializer.StringEncoder");
        props.put("partitioner.class", "kafka.producer.DefaultPartitioner");
        props.put("producer.type", "sync");
        props.put("compression.codec", "none");
        props.put("request.required.acks", "1");
        props.put("batch.num.messages", "1024");
        props.put("client.id", "");

        if (producerSettings != null) {
            props.putAll(producerSettings);
        }
        props.put("metadata.broker.list", brokerList);

        ProducerConfig producerConfig = new ProducerConfig(props);
        producer = new Producer<>(producerConfig);
    }

    @Override
    protected void emit(Map event) throws IOException {
        String tp = Formatter.format(event, topic, timezone);
        producer.send(new KeyedMessage<>(tp, event.toString(), objectMapper.writeValueAsString(event).getBytes(encoding)));
    }

    @Override
    public void closeInternal() throws IOException {
        LOG.warn("kafka output closeInternal.");
        producer.close();
    }

    public void setEncoding(String encoding) {
        this.encoding = encoding;
    }

    public void setBrokerList(String brokerList) {
        this.brokerList = brokerList;
    }
}
