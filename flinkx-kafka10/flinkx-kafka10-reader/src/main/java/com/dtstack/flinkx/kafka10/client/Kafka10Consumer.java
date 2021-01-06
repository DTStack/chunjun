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
package com.dtstack.flinkx.kafka10.client;

import com.dtstack.flinkx.constants.ConstantValue;
import com.dtstack.flinkx.kafkabase.KafkaConfigKeys;
import com.dtstack.flinkx.kafkabase.KafkaInputSplit;
import com.dtstack.flinkx.kafkabase.client.KafkaBaseConsumer;
import com.dtstack.flinkx.kafkabase.format.KafkaBaseInputFormat;

import java.util.Arrays;
import java.util.Properties;

/**
 * @company: www.dtstack.com
 * @author: toutian
 * @create: 2019/7/4
 */
public class Kafka10Consumer extends KafkaBaseConsumer {

    public Kafka10Consumer(Properties properties) {
        super(properties);
    }

    @Override
    public Kafka10Consumer createClient(String topic, String group, KafkaBaseInputFormat format, KafkaInputSplit kafkaInputSplit) {
        Properties clientProps = new Properties();
        clientProps.putAll(props);
        clientProps.put(KafkaConfigKeys.GROUP_ID, group);

        client = new Kafka10Client(clientProps, Arrays.asList(topic.split(ConstantValue.COMMA_SYMBOL)), Long.MAX_VALUE, format);
        return this;
    }
}
