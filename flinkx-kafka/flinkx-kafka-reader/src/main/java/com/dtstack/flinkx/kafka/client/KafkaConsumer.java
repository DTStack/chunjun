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
package com.dtstack.flinkx.kafka.client;

import com.dtstack.flinkx.kafkabase.KafkaConfigKeys;
import com.dtstack.flinkx.kafkabase.KafkaInputSplit;
import com.dtstack.flinkx.kafkabase.client.KafkaBaseConsumer;
import com.dtstack.flinkx.kafkabase.entity.kafkaState;
import com.dtstack.flinkx.kafkabase.format.KafkaBaseInputFormat;

import java.util.Collection;
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
    public KafkaBaseConsumer createClient(String topic, String group, KafkaBaseInputFormat format, KafkaInputSplit kafkaInputSplit) {
        Properties clientProps = new Properties();
        clientProps.putAll(props);
        clientProps.put(KafkaConfigKeys.GROUP_ID, group);

        client = new KafkaClient(clientProps, 100L, format, kafkaInputSplit);
        return this;
    }

    /**
     * 提交kafka offset
     * @param kafkaStates
     */
    public void submitOffsets(Collection<kafkaState> kafkaStates){
        if(client != null){
            KafkaClient kafkaClient = (KafkaClient) this.client;
            kafkaClient.submitOffsets(kafkaStates);
        }
    }
}
