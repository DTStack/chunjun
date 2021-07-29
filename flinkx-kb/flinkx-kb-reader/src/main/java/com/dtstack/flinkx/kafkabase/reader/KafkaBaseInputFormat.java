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
package com.dtstack.flinkx.kafkabase.reader;

import com.dtstack.flinkx.decoder.DecodeEnum;
import com.dtstack.flinkx.decoder.IDecode;
import com.dtstack.flinkx.decoder.JsonDecoder;
import com.dtstack.flinkx.decoder.PlainDecoder;
import com.dtstack.flinkx.inputformat.BaseRichInputFormat;
import com.dtstack.flinkx.util.ExceptionUtil;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;

/**
 * Date: 2019/11/21
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public class KafkaBaseInputFormat extends BaseRichInputFormat {

    protected static final Logger LOG = LoggerFactory.getLogger(KafkaBaseInputFormat.class);

    protected String topic;
    protected String groupId;
    protected String codec;
    protected boolean blankIgnore;
    protected String encoding;
    protected Map<String, String> consumerSettings;
    protected volatile boolean running = false;
    protected transient BlockingQueue<Row> queue;
    protected transient KafkaBaseConsumer consumer;
    protected transient IDecode decode;

    @Override
    public void openInputFormat() throws IOException {
        super.openInputFormat();

        queue = new SynchronousQueue<>(false);
        if (DecodeEnum.JSON.getName().equalsIgnoreCase(codec)) {
            decode = new JsonDecoder();
        } else {
            decode = new PlainDecoder();
        }
    }

    @Override
    protected void openInternal(InputSplit inputSplit) throws IOException {
        consumer.createClient(topic, groupId, this).execute();
        running = true;
    }

    @Override
    protected Row nextRecordInternal(Row row) throws IOException {
        try {
            row = queue.take();
        } catch (InterruptedException e) {
            LOG.error("takeEvent interrupted error:{}", ExceptionUtil.getErrorMessage(e));
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

    public void processEvent(Map<String, Object> event) {
        try {
            queue.put(Row.of(event));
        } catch (InterruptedException e) {
            LOG.error("takeEvent interrupted event:{} error:{}", event, e);
        }
    }

    protected Properties geneConsumerProp() {
        Properties props = new Properties();
        for (Map.Entry<String, String> entry : consumerSettings.entrySet()) {
            String k = entry.getKey();
            String v = entry.getValue();
            props.put(k, v);
        }
        return props;
    }

    @Override
    protected InputSplit[] createInputSplitsInternal(int minNumSplits) throws Exception {
        InputSplit[] splits = new InputSplit[minNumSplits];
        for (int i = 0; i < minNumSplits; i++) {
            splits[i] = new GenericInputSplit(i, minNumSplits);
        }
        return splits;
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

    public void setBlankIgnore(boolean blankIgnore) {
        this.blankIgnore = blankIgnore;
    }

    public boolean getBlankIgnore() {
        return blankIgnore;
    }

    public void setConsumerSettings(Map<String, String> consumerSettings) {
        this.consumerSettings = consumerSettings;
    }

    public String getEncoding() {
        return encoding;
    }

    public void setEncoding(String encoding) {
        this.encoding = encoding;
    }

    public IDecode getDecode() {
        return decode;
    }
}
