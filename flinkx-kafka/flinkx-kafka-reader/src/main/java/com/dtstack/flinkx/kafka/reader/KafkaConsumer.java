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
package com.dtstack.flinkx.kafka.reader;

import com.dtstack.flinkx.kafkabase.reader.KafkaBaseConsumer;
import com.dtstack.flinkx.kafkabase.reader.KafkaBaseInputFormat;

import java.util.Arrays;
import java.util.Properties;

/**
 * Date: 2019/11/21
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public class KafkaConsumer extends KafkaBaseConsumer {
    public KafkaConsumer(Properties properties) {
        super(properties);
    }

    @Override
    public KafkaBaseConsumer createClient(String topic, String group, KafkaBaseInputFormat format) {
        Properties clientProps = new Properties();
        clientProps.putAll(props);
        clientProps.put("group.id", group);

        client = new KafkaClient(clientProps, Arrays.asList(topic.split(",")), Long.MAX_VALUE, format);
        return this;
    }
}
