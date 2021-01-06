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
import com.dtstack.flinkx.kafka09.format.Kafka09InputFormat;
import com.dtstack.flinkx.kafkabase.KafkaConfigKeys;
import com.dtstack.flinkx.kafkabase.format.KafkaBaseInputFormatBuilder;
import com.dtstack.flinkx.kafkabase.reader.KafkaBaseReader;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @company: www.dtstack.com
 * @author: toutian
 * @create: 2019/7/4
 */
public class Kafka09Reader extends KafkaBaseReader {

    public Kafka09Reader(DataTransferConfig config, StreamExecutionEnvironment env) {
        super(config, env);
        //兼容历史脚本
        String id = consumerSettings.get(KafkaConfigKeys.GROUP_ID);
        if(StringUtils.isNotBlank(id)){
            super.groupId = id;
        }
    }

    @Override
    public KafkaBaseInputFormatBuilder getBuilder(){
        return new KafkaBaseInputFormatBuilder(new Kafka09InputFormat());
    }
}
