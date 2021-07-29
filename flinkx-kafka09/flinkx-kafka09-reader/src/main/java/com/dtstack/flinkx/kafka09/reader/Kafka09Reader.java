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

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.config.ReaderConfig;
import com.dtstack.flinkx.kafkabase.KafkaConfigKeys;
import com.dtstack.flinkx.kafkabase.reader.KafkaBaseInputFormat;
import com.dtstack.flinkx.kafkabase.reader.KafkaBaseReader;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.nio.charset.StandardCharsets;

/**
 * @company: www.dtstack.com
 * @author: toutian
 * @create: 2019/7/4
 */
public class Kafka09Reader extends KafkaBaseReader {
    private String encoding;

    public Kafka09Reader(DataTransferConfig config, StreamExecutionEnvironment env) {
        super(config, env);
        ReaderConfig readerConfig = config.getJob().getContent().get(0).getReader();
        encoding = readerConfig.getParameter().getStringVal(KafkaConfigKeys.KEY_ENCODING, StandardCharsets.UTF_8.name());
    }

    @Override
    public KafkaBaseInputFormat getFormat(){
        Kafka09InputFormat format = new Kafka09InputFormat();
        format.setEncoding(encoding);
        return format;
    }
}
