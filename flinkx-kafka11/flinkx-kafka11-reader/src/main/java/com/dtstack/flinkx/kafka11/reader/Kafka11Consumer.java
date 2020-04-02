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
package com.dtstack.flinkx.kafka11.reader;

import com.dtstack.flinkx.kafkabase.reader.KafkaBaseConsumer;
import com.dtstack.flinkx.kafkabase.reader.KafkaBaseInputFormat;

import java.util.Arrays;
import java.util.Properties;

/**
 * @company: www.dtstack.com
 * @author: toutian
 * @create: 2019/7/4
 */
public class Kafka11Consumer extends KafkaBaseConsumer {

    public Kafka11Consumer(Properties properties) {
        super(properties);
    }

    @Override
    public KafkaBaseConsumer createClient(String topic, String group, KafkaBaseInputFormat format) {
        Properties clientProps = new Properties();
        clientProps.putAll(props);
        clientProps.put("group.id", group);

        client = new Kafka11Client(clientProps, Arrays.asList(topic.split(",")), Long.MAX_VALUE, format);
        return this;
    }
}
