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
package com.dtstack.flinkx.kafkabase.writer;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.config.WriterConfig;
import com.dtstack.flinkx.kafkabase.format.KafkaBaseOutputFormat;
import com.dtstack.flinkx.writer.BaseDataWriter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.types.Row;

import java.util.List;
import java.util.Map;

import static com.dtstack.flinkx.kafkabase.KafkaConfigKeys.KEY_PRODUCER_SETTINGS;
import static com.dtstack.flinkx.kafkabase.KafkaConfigKeys.KEY_TABLE_FIELDS;
import static com.dtstack.flinkx.kafkabase.KafkaConfigKeys.KEY_TIMEZONE;
import static com.dtstack.flinkx.kafkabase.KafkaConfigKeys.KEY_TOPIC;

/**
 * Date: 2019/11/21
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public class KafkaBaseWriter extends BaseDataWriter {
    protected String timezone;
    protected String topic;
    protected Map<String, String> producerSettings;
    protected List<String> tableFields;

    @SuppressWarnings("unchecked")
    public KafkaBaseWriter(DataTransferConfig config) {
        super(config);
        WriterConfig writerConfig = config.getJob().getContent().get(0).getWriter();
        timezone = writerConfig.getParameter().getStringVal(KEY_TIMEZONE);
        topic = writerConfig.getParameter().getStringVal(KEY_TOPIC);
        producerSettings = (Map<String, String>) writerConfig.getParameter().getVal(KEY_PRODUCER_SETTINGS);
        tableFields = (List<String>)writerConfig.getParameter().getVal(KEY_TABLE_FIELDS);
    }

    @Override
    public DataStreamSink<?> writeData(DataStream<Row> dataSet) {
        KafkaBaseOutputFormat format = new KafkaBaseOutputFormat();
        format.setTimezone(timezone);
        format.setTopic(topic);
        format.setProducerSettings(producerSettings);
        format.setRestoreConfig(restoreConfig);
        format.setTableFields(tableFields);
        format.setDirtyPath(dirtyPath);
        format.setDirtyHadoopConfig(dirtyHadoopConfig);
        format.setSrcFieldNames(srcCols);
        return createOutput(dataSet, format);
    }
}
