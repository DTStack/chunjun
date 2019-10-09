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

import com.dtstack.flinkx.config.RestoreConfig;
import com.dtstack.flinkx.inputformat.RichInputFormat;
import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;

/**
 * company: www.dtstack.com
 * author: toutian
 * create: 2019/7/5
 */
public class Kafka11InputFormat extends RichInputFormat {

    private static final Logger LOG = LoggerFactory.getLogger(Kafka11InputFormat.class);

    private String topic;

    private String groupId;

    private String codec;

    private boolean blankIgnore;

    private Map<String, String> consumerSettings;

    private volatile boolean running = false;

    private transient BlockingQueue<Row> queue;

    private transient KafkaConsumer consumer;

    @Override
    protected void openInternal(InputSplit inputSplit) throws IOException {
        consumer.createClient(topic, groupId, this).execute();
        running = true;
    }

    public void processEvent(Map<String, Object> event) {
        try {
            queue.put(Row.of(event));
        } catch (InterruptedException e) {
            LOG.error("takeEvent interrupted event:{} error:{}", event, e);
        }
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
            consumer.close();
            running = false;
            LOG.warn("input kafka release.");
        }
    }

    @Override
    public void configure(Configuration parameters) {
        Properties props = geneConsumerProp();

        consumer = new KafkaConsumer(props);
        queue = new SynchronousQueue<Row>(false);
    }

    private Properties geneConsumerProp() {
        Properties props = new Properties();

        Iterator<Map.Entry<String, String>> consumerSetting = consumerSettings
                .entrySet().iterator();

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


    public void setTopic(String topic) {
        this.topic = topic;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public void setCodec(String codec) {
        this.codec = codec;
    }

    public String getCodec() {
        return codec;
    }


    public void setBlankIgnore(boolean blankIgnore) {
        this.blankIgnore = blankIgnore;
    }

    public boolean getBlankIgnore() {
        return blankIgnore;
    }

    public void setConsumerSettings(Map<String, String> consumerSettings) {
        this.consumerSettings = consumerSettings;
    }

    public void setRestoreConfig(RestoreConfig restoreConfig) {
        this.restoreConfig = restoreConfig;
    }
}
