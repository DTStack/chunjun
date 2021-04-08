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
package com.dtstack.flinkx.kafkabase.reader;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.config.ReaderConfig;
import com.dtstack.flinkx.kafkabase.KafkaConfigKeys;
import com.dtstack.flinkx.kafkabase.enums.StartupMode;
import com.dtstack.flinkx.kafkabase.format.KafkaBaseInputFormat;
import com.dtstack.flinkx.kafkabase.format.KafkaBaseInputFormatBuilder;
import com.dtstack.flinkx.reader.BaseDataReader;
import com.dtstack.flinkx.reader.MetaColumn;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

import java.util.List;
import java.util.Map;

/**
 * Date: 2019/11/21
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public class KafkaBaseReader extends BaseDataReader {
    protected String topic;
    protected String groupId;
    protected String codec;
    protected boolean blankIgnore;
    protected String encoding;
    protected String mode;
    protected String offset;
    protected Long timestamp;
    protected Map<String, String> consumerSettings;
    protected List<MetaColumn> metaColumns;

    @SuppressWarnings("unchecked")
    public KafkaBaseReader(DataTransferConfig config, StreamExecutionEnvironment env) {
        super(config, env);
        ReaderConfig readerConfig = config.getJob().getContent().get(0).getReader();
        topic = readerConfig.getParameter().getStringVal(KafkaConfigKeys.KEY_TOPIC);
        groupId = readerConfig.getParameter().getStringVal(KafkaConfigKeys.KEY_GROUP_ID, "default");
        codec = readerConfig.getParameter().getStringVal(KafkaConfigKeys.KEY_CODEC, "text");
        blankIgnore = readerConfig.getParameter().getBooleanVal(KafkaConfigKeys.KEY_BLANK_IGNORE, false);
        mode = readerConfig.getParameter().getStringVal(KafkaConfigKeys.KEY_MODE, StartupMode.GROUP_OFFSETS.name);
        offset = readerConfig.getParameter().getStringVal(KafkaConfigKeys.KEY_OFFSET, "");
        timestamp = readerConfig.getParameter().getLongVal(KafkaConfigKeys.KEY_TIMESTAMP, -1L);
        consumerSettings = (Map<String, String>) readerConfig.getParameter().getVal(KafkaConfigKeys.KEY_CONSUMER_SETTINGS);
        metaColumns = MetaColumn.getMetaColumns(readerConfig.getParameter().getColumn());
    }

    @Override
    public DataStream<Row> readData() {
        KafkaBaseInputFormatBuilder builder = getBuilder();
        builder.setDataTransferConfig(dataTransferConfig);
        builder.setRestoreConfig(restoreConfig);
        builder.setTopic(topic);
        builder.setGroupId(groupId);
        builder.setCodec(codec);
        builder.setBlankIgnore(blankIgnore);
        builder.setConsumerSettings(consumerSettings);
        builder.setMode(StartupMode.getFromName(mode));
        builder.setOffset(offset);
        builder.setTimestamp(timestamp);
        builder.setMetaColumns(metaColumns);
        return createInput(builder.finish());
    }

    /**
     * 获取不同版本的kafkaInputFormat
     * @return
     */
    public KafkaBaseInputFormatBuilder getBuilder(){
        return new KafkaBaseInputFormatBuilder(new KafkaBaseInputFormat());
    }
}
