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
package com.dtstack.flinkx.kafka10.client;

import com.dtstack.flinkx.decoder.IDecode;
import com.dtstack.flinkx.kafkabase.client.IClient;
import com.dtstack.flinkx.kafkabase.entity.kafkaState;
import com.dtstack.flinkx.kafkabase.format.KafkaBaseInputFormat;
import com.dtstack.flinkx.util.ExceptionUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Date: 2019/12/25
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public class Kafka10Client implements IClient {
    protected static Logger LOG = LoggerFactory.getLogger(Kafka10Client.class);
    private volatile boolean running = true;
    private long pollTimeout;
    private boolean blankIgnore;
    private IDecode decode;
    private KafkaBaseInputFormat format;
    private KafkaConsumer<String, String> consumer;

    public Kafka10Client(Properties clientProps, List<String> topics, long pollTimeout, KafkaBaseInputFormat format) {
        this.pollTimeout = pollTimeout;
        this.blankIgnore = format.getBlankIgnore();
        this.format = format;
        this.decode = format.getDecode();
        consumer = new KafkaConsumer<>(clientProps);
        consumer.subscribe(topics);
    }

    @Override
    public void run() {
        Thread.currentThread().setUncaughtExceptionHandler((t, e) -> {
            LOG.warn("KafkaClient run failed, Throwable = {}", ExceptionUtil.getErrorMessage(e));
        });
        try {
            while (running) {
                ConsumerRecords<String, String> records = consumer.poll(pollTimeout);
                for (ConsumerRecord<String, String> r : records) {
                    boolean isIgnoreCurrent = r.value() == null || blankIgnore && StringUtils.isBlank(r.value());
                    if (isIgnoreCurrent) {
                        continue;
                    }

                    try {
                        processMessage(r.value(), r.topic(), r.partition(), r.offset(), r.timestamp());
                    } catch (Throwable e) {
                        LOG.error("kafka consumer fetch is error, message = {}, e = {}", r.value(), ExceptionUtil.getErrorMessage(e));
                    }
                }
            }
        } catch (WakeupException e) {
            LOG.warn("WakeupException to close kafka consumer, e = {}", ExceptionUtil.getErrorMessage(e));
        } catch (Throwable e) {
            LOG.error("kafka consumer fetch is error, e = {}", ExceptionUtil.getErrorMessage(e));
        } finally {
            consumer.close();
        }
    }

    @Override
    public void processMessage(String message, String topic, Integer partition, Long offset, Long timestamp) {
        Map<String, Object> event = decode.decode(message);
        if (event != null && event.size() > 0) {
            format.processEvent(Pair.of(event, new kafkaState(topic, partition, offset, timestamp)));
        }
    }

    @Override
    public void close() {
        try {
            running = false;
            consumer.wakeup();
        } catch (Exception e) {
            LOG.error("close kafka consumer error", e);
        }
    }

}
