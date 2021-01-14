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
package com.dtstack.flinkx.kafka.writer;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.kafka.format.KafkaOutputFormat;
import com.dtstack.flinkx.kafkabase.writer.HeartBeatController;
import com.dtstack.flinkx.kafkabase.writer.KafkaBaseWriter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.producer.ProducerConfig;

/**
 * Date: 2019/11/21
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public class KafkaWriter extends KafkaBaseWriter {

    public KafkaWriter(DataTransferConfig config) {
        super(config);
        if (!producerSettings.containsKey(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG)) {
            throw new IllegalArgumentException(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG + " must set in producerSettings");
        }
    }

    @Override
    public DataStreamSink<?> writeData(DataStream<Row> dataSet) {
        KafkaOutputFormat format = new KafkaOutputFormat();
        format.setTimezone(timezone);
        format.setTopic(topic);
        format.setProducerSettings(producerSettings);
        format.setRestoreConfig(restoreConfig);
        format.setTableFields(tableFields);
        format.setDirtyPath(dirtyPath);
        format.setDirtyHadoopConfig(dirtyHadoopConfig);
        format.setSrcFieldNames(srcCols);
        format.setHeartBeatController(new HeartBeatController());

        return createOutput(dataSet, format);
    }
}
