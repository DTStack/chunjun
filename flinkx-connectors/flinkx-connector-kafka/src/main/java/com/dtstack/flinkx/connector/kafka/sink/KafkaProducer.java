/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.flinkx.connector.kafka.sink;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.table.data.RowData;

import java.util.Properties;

/**
 * @author chuixue
 * @create 2021-05-08 10:01
 * @description
 */
public class KafkaProducer extends FlinkKafkaProducer<RowData> {

    private KafkaSerializationSchema<RowData> serializationSchema;

    public KafkaProducer(
            String defaultTopic,
            KafkaSerializationSchema<RowData> serializationSchema,
            Properties producerConfig,
            FlinkKafkaProducer.Semantic semantic,
            int kafkaProducersPoolSize) {
        super(defaultTopic, serializationSchema, producerConfig, semantic, kafkaProducersPoolSize);
        this.serializationSchema = serializationSchema;
    }

    @Override
    public void open(Configuration configuration) throws Exception {
        RuntimeContext runtimeContext = getRuntimeContext();
        ((DynamicKafkaSerializationSchema) serializationSchema).setRuntimeContext(runtimeContext);
        ((DynamicKafkaSerializationSchema) serializationSchema).initMetric();
        super.open(configuration);
    }
}
