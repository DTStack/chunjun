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
package com.dtstack.flinkx.kafka11.format;

import com.dtstack.flinkx.kafkabase.util.Formatter;
import com.dtstack.flinkx.kafkabase.format.KafkaBaseOutputFormat;
import com.dtstack.flinkx.util.ExceptionUtil;
import com.dtstack.flinkx.util.MapUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * @company: www.dtstack.com
 * @author: toutian
 * @create: 2019/7/5
 */
public class Kafka11OutputFormat extends KafkaBaseOutputFormat {

    private transient KafkaProducer<String, String> producer;

    @Override
    public void configure(Configuration parameters) {
        super.configure(parameters);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 60000);
        props.put(ProducerConfig.RETRIES_CONFIG, 1000000);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
        if (producerSettings != null) {
            props.putAll(producerSettings);
        }
        producer = new KafkaProducer<>(props);
    }

    @Override
    protected void emit(Map event) throws IOException {
        heartBeatController.acquire();
        String tp = Formatter.format(event, topic, timezone);
        producer.send(new ProducerRecord<>(tp, event.toString(), MapUtil.writeValueAsString(event)), (metadata, exception) -> {
            if(Objects.nonNull(exception)){
                String errorMessage = String.format("send data failed,data 【%s】 ,error info  %s",event,ExceptionUtil.getErrorMessage(exception));
                LOG.warn(errorMessage);
                heartBeatController.onFailed(exception);
            } else {
                heartBeatController.onSuccess();
            }
        });
    }

    @Override
    public void closeInternal() {
        LOG.warn("kafka output closeInternal.");
        //未设置具体超时时间 关闭时间默认是long.value  导致整个方法长时间等待关闭不了，因此明确指定20s时间
        producer.close(KafkaBaseOutputFormat.CLOSE_TIME, TimeUnit.MILLISECONDS);
    }
}
