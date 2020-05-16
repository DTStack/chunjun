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
import com.dtstack.flinkx.reader.BaseDataReader;
import com.dtstack.flinkx.reader.MetaColumn;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

import java.util.List;
import java.util.Map;

import static com.dtstack.flinkx.kafkabase.KafkaConfigKeys.*;

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
    protected Map<String, String> consumerSettings;
    protected List<MetaColumn> metaColumns;

    @SuppressWarnings("unchecked")
    public KafkaBaseReader(DataTransferConfig config, StreamExecutionEnvironment env) {
        super(config, env);
        ReaderConfig readerConfig = config.getJob().getContent().get(0).getReader();
        topic = readerConfig.getParameter().getStringVal(KEY_TOPIC);
        groupId = readerConfig.getParameter().getStringVal(KEY_GROUP_ID);
        codec = readerConfig.getParameter().getStringVal(KEY_CODEC, "plain");
        blankIgnore = readerConfig.getParameter().getBooleanVal(KEY_BLANK_IGNORE, false);
        consumerSettings = (Map<String, String>) readerConfig.getParameter().getVal(KEY_CONSUMER_SETTINGS);
        metaColumns = MetaColumn.getMetaColumns(readerConfig.getParameter().getColumn());
    }

    @Override
    public DataStream<Row> readData() {
        KafkaBaseInputFormat format = getFormat();
        format.setTopic(topic);
        format.setGroupId(groupId);
        format.setCodec(codec);
        format.setBlankIgnore(blankIgnore);
        format.setConsumerSettings(consumerSettings);
        format.setRestoreConfig(restoreConfig);
        format.setMetaColumns(metaColumns);
        return createInput(format);
    }

    public KafkaBaseInputFormat getFormat(){
        return new KafkaBaseInputFormat();
    }
}
