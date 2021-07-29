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
package com.dtstack.flinkx.kafka11.reader;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.kafkabase.reader.KafkaBaseInputFormat;
import com.dtstack.flinkx.kafkabase.reader.KafkaBaseReader;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.ProducerConfig;

/**
 * @company: www.dtstack.com
 * @author: toutian
 * @create: 2019/7/4
 */
public class Kafka11Reader extends KafkaBaseReader {

    public Kafka11Reader(DataTransferConfig config, StreamExecutionEnvironment env) {
        super(config, env);
        if (!consumerSettings.containsKey(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG)){
            throw new IllegalArgumentException(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG + " must set in consumerSettings");
        }
    }

    @Override
    public KafkaBaseInputFormat getFormat(){
        return new Kafka11InputFormat();
    }
}
