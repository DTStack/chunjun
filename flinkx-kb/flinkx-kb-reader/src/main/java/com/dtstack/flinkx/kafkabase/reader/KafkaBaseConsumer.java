/*
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
package com.dtstack.flinkx.kafkabase.reader;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

/**
 * Date: 2019/12/25
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public class KafkaBaseConsumer {
    protected Properties props;

    protected IClient client;

    protected ExecutorService executor = new ScheduledThreadPoolExecutor(1, new BasicThreadFactory
            .Builder()
            .namingPattern("KafkaConsumerThread-%d")
            .daemon(true)
            .build());

    public KafkaBaseConsumer(Properties properties) {
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

    public KafkaBaseConsumer createClient(String topic, String group, KafkaBaseInputFormat format){
        throw new RuntimeException("KafkaBaseConsumer.createClient() should be override by subclass!");
    }

    public void execute() {
        executor.execute(client);
    }

    public void close() {
        if (client != null) {
            client.close();
        }
    }

}
