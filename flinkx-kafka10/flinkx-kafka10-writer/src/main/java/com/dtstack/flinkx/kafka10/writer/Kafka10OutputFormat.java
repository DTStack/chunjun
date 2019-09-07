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

package com.dtstack.flinkx.kafka10.writer;

import com.dtstack.flinkx.config.RestoreConfig;
import com.dtstack.flinkx.exception.WriteRecordException;
import com.dtstack.flinkx.kafka10.Formatter;
import com.dtstack.flinkx.kafka10.decoder.JsonDecoder;
import com.dtstack.flinkx.outputformat.RichOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

/**
 * company: www.dtstack.com
 * author: toutian
 * create: 2019/7/5
 */
public class Kafka10OutputFormat extends RichOutputFormat {

    private static final Logger LOG = LoggerFactory.getLogger(Kafka10OutputFormat.class);

    private Properties props;

    private String timezone;

    private String topic;

    private Map<String, String> producerSettings;

    private transient KafkaProducer<String, String> producer;

    private transient JsonDecoder jsonDecoder = new JsonDecoder();

    private transient static ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Configuration parameters) {
        props = new Properties();
        addDefaultKafkaSetting();
        if (producerSettings != null) {
            props.putAll(producerSettings);
        }
        producer = new KafkaProducer<>(props);
    }

    private void addDefaultKafkaSetting() {
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 86400000);
        props.put(ProducerConfig.RETRIES_CONFIG, 1000000);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
    }

    @Override
    protected void openInternal(int taskNumber, int numTasks) throws IOException {

    }

    @Override
    protected void writeSingleRecordInternal(Row row) throws WriteRecordException {
        try {
            if (row.getArity() == 1) {
                Object obj = row.getField(0);
                if (obj instanceof Map) {
                    emit((Map<String, Object>) obj);
                } else if (obj instanceof String) {
                    emit(jsonDecoder.decode(obj.toString()));
                }
            }
        } catch (Throwable e) {
            LOG.error("kafka writeSingleRecordInternal error:{}", e);
        }
    }

    private void emit(Map event) throws IOException {
        String tp = Formatter.format(event, topic, timezone);
        producer.send(new ProducerRecord<String, String>(tp, event.toString(), objectMapper.writeValueAsString(event)));
    }

    @Override
    public void closeInternal() throws IOException {
        LOG.warn("kafka output closeInternal.");
        producer.close();
    }

    @Override
    protected void writeMultipleRecordsInternal() throws Exception {
        throw new UnsupportedOperationException();
    }


    public void setTimezone(String timezone) {
        this.timezone = timezone;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public void setProducerSettings(Map<String, String> producerSettings) {
        this.producerSettings = producerSettings;
    }


    public void setRestoreConfig(RestoreConfig restoreConfig) {
        this.restoreConfig = restoreConfig;
    }
}
