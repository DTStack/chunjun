/**
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
package com.dtstack.flinkx.kafka10.reader;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * company: www.dtstack.com
 * author: toutian
 * create: 2019/7/4
 */
public class KafkaConsumer {

    private static Logger LOG = LoggerFactory.getLogger(KafkaConsumer.class);

    private Properties props;

    private transient Client client;

    private transient ExecutorService executor = Executors.newSingleThreadExecutor();

    public KafkaConsumer(Properties properties) {
        Properties props = new Properties();
        props.put("max.poll.interval.ms", "86400000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "earliest");
        if (properties != null) {
            props.putAll(properties);
        }

        this.props = props;
    }

    public KafkaConsumer createClient(String topic, String group, Caller caller, long timeout) {
        Properties clientProps = new Properties();
        clientProps.putAll(props);
        clientProps.put("group.id", group);

        client = new Client(clientProps, Arrays.asList(topic.split(",")), caller, timeout);
        return this;
    }

    public KafkaConsumer createClient(String topics, String group, Caller caller) {
        return createClient(topics, group, caller, Long.MAX_VALUE);
    }

    public void execute() {
        executor.execute(client);
    }

    public interface Caller {

        void processMessage(String message);

        void catchException(String message, Throwable e);
    }

    public class Client implements Runnable {

        private Caller caller;

        private volatile boolean running = true;

        private long pollTimeout;

        private org.apache.kafka.clients.consumer.KafkaConsumer<String, String> consumer;

        public Client(Properties clientProps, List<String> topics, Caller caller, long pollTimeout) {

            this.pollTimeout = pollTimeout;
            this.caller = caller;

            consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(clientProps);
            consumer.subscribe(topics);
        }

        @Override
        public void run() {

            try {

                while (running) {

                    ConsumerRecords<String, String> records = consumer.poll(pollTimeout);
                    for (ConsumerRecord<String, String> r : records) {

                        if (r.value() == null || "".equals(r.value())) {
                            continue;
                        }

                        try {
                            caller.processMessage(r.value());

                        } catch (Throwable e) {
                            caller.catchException(r.value(), e);
                        }
                    }
                }

            } catch (WakeupException e) {
                LOG.warn("WakeupException to close kafka consumer");
            } catch (Throwable e) {
                caller.catchException("", e);
            } finally {
                consumer.close();
            }
        }

        public void close() {
            try {
                running = false;
                consumer.wakeup();
            } catch (Exception e) {
                LOG.error("close kafka consumer error", e);
            }
        }
    }

    public void close() {
        client.close();
    }

}
