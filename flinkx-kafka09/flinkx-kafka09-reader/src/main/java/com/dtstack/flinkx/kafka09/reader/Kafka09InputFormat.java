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

import com.dtstack.flinkx.config.RestoreConfig;
import com.dtstack.flinkx.inputformat.RichInputFormat;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;

/**
 * company: www.dtstack.com
 * author: toutian
 * create: 2019/7/5
 */
public class Kafka09InputFormat extends RichInputFormat {

    private static final Logger LOG = LoggerFactory.getLogger(Kafka09InputFormat.class);

    private String encoding;

    private String codec;

    private String topic;

    private Map<String, String> consumerSettings;

    private volatile boolean running = false;

    private transient BlockingQueue<Row> queue;

    private transient ExecutorService executor;

    private transient ConsumerConnector consumerConnector;

    @Override
    protected void openInternal(InputSplit inputSplit) throws IOException {
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, 1);
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumerConnector.createMessageStreams(topicCountMap);

        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
        for (final KafkaStream<byte[], byte[]> stream : streams) {
            executor.submit(new KafkaConsumer(stream, this));
        }
        running = true;
    }

    @Override
    protected Row nextRecordInternal(Row row) throws IOException {
        try {
            row = queue.take();
        } catch (InterruptedException e) {
            LOG.error("takeEvent interrupted error:{}", e);
        }
        return row;
    }

    @Override
    protected void closeInternal() throws IOException {
        if (running) {
            consumerConnector.commitOffsets(true);
            consumerConnector.shutdown();
            executor.shutdownNow();
            running = false;
            LOG.warn("input kafka release.");
        }
    }

    @Override
    public void configure(Configuration parameters) {
        Properties props = geneConsumerProp();
        consumerConnector = kafka.consumer.Consumer.createJavaConsumerConnector(new ConsumerConfig(props));

        executor = Executors.newFixedThreadPool(1);
        queue = new SynchronousQueue<Row>(false);
    }

    private Properties geneConsumerProp() {
        Properties props = new Properties();
        Iterator<Map.Entry<String, String>> consumerSetting = consumerSettings.entrySet().iterator();
        while (consumerSetting.hasNext()) {
            Map.Entry<String, String> entry = consumerSetting.next();
            String k = entry.getKey();
            String v = entry.getValue();
            props.put(k, v);
        }
        return props;
    }

    @Override
    public InputSplit[] createInputSplits(int minNumSplits) throws IOException {
        InputSplit[] splits = new InputSplit[minNumSplits];
        for (int i = 0; i < minNumSplits; i++) {
            splits[i] = new GenericInputSplit(i, minNumSplits);
        }
        return splits;
    }

    @Override
    public InputSplitAssigner getInputSplitAssigner(InputSplit[] inputSplits) {
        return new DefaultInputSplitAssigner(inputSplits);
    }

    @Override
    public boolean reachedEnd() throws IOException {
        return false;
    }

    public void processEvent(Map<String, Object> event) {
        try {
            queue.put(Row.of(event));
        } catch (InterruptedException e) {
            LOG.error("takeEvent interrupted event:{} error:{}", event, e);
        }
    }

    public void setEncoding(String encoding) {
        this.encoding = encoding;
    }

    public String getEncoding() {
        return encoding;
    }

    public String getCodec() {
        return codec;
    }

    public void setCodec(String codec) {
        this.codec = codec;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public void setConsumerSettings(Map<String, String> consumerSettings) {
        this.consumerSettings = consumerSettings;
    }

    public void setRestoreConfig(RestoreConfig restoreConfig) {
        this.restoreConfig = restoreConfig;
    }
}
