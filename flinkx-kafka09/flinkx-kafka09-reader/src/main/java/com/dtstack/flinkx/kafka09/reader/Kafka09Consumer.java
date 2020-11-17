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
package com.dtstack.flinkx.kafka09.reader;

import com.dtstack.flinkx.kafkabase.reader.KafkaBaseConsumer;
import com.dtstack.flinkx.kafkabase.reader.KafkaBaseInputFormat;
import kafka.consumer.KafkaStream;

import java.util.Properties;

/**
 * @company: www.dtstack.com
 * @author: toutian
 * @create: 2019/7/5
 */
public class Kafka09Consumer extends KafkaBaseConsumer {
    private KafkaStream<byte[], byte[]> mStream;

    public Kafka09Consumer(KafkaStream<byte[], byte[]> aStream) {
        super(new Properties());
        this.mStream = aStream;
    }

    @Override
    public KafkaBaseConsumer createClient(String topic, String group, KafkaBaseInputFormat format) {
        client = new Kafka09Client(mStream, format);
        return this;
    }
}
