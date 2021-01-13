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
package com.dtstack.flinkx.kafkabase.format;

import com.dtstack.flinkx.constants.ConstantValue;
import com.dtstack.flinkx.inputformat.BaseRichInputFormatBuilder;
import com.dtstack.flinkx.kafkabase.KafkaConfigKeys;
import com.dtstack.flinkx.kafkabase.enums.KafkaVersion;
import com.dtstack.flinkx.kafkabase.enums.StartupMode;
import com.dtstack.flinkx.kafkabase.util.KafkaUtil;
import com.dtstack.flinkx.reader.MetaColumn;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;


/**
 * Date: 2020/12/07
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public class KafkaBaseInputFormatBuilder extends BaseRichInputFormatBuilder {

    private KafkaBaseInputFormat format;

    public KafkaBaseInputFormatBuilder(KafkaBaseInputFormat format) {
        super.format = this.format = format;
    }

    public void setTopic(String topic) {
        format.topic = topic;
    }

    public void setGroupId(String groupId) {
        format.groupId = groupId;
    }

    public void setCodec(String codec) {
        format.codec = codec;
    }

    public void setBlankIgnore(boolean blankIgnore) {
        format.blankIgnore = blankIgnore;
    }

    public void setConsumerSettings(Map<String, String> consumerSettings) {
        format.consumerSettings = consumerSettings;
    }

    public void setMode(StartupMode mode) {
        format.mode = mode;
    }

    public void setOffset(String offset) {
        format.offset = offset;
    }

    public void setTimestamp(Long timestamp) {
        format.timestamp = timestamp;
    }

    public void setMetaColumns(List<MetaColumn> metaColumns) {
        format.metaColumns = metaColumns;
    }

    @Override
    protected void checkFormat() {
        StringBuilder sb = new StringBuilder(128);
        if(StringUtils.isBlank(format.topic)){
            sb.append("No kafka topic supplied;\n");
        }

        if(StartupMode.TIMESTAMP.equals(format.mode) || StartupMode.SPECIFIC_OFFSETS.equals(format.mode)){
            if(format.topic.split(ConstantValue.COMMA_SYMBOL).length > 1){
                sb.append("mode [")
                        .append(format.mode)
                        .append("] is not supported when the number of kafka topic bigger than 1;\n");
            }else if(StartupMode.TIMESTAMP.equals(format.mode) && format.timestamp < 0){
                sb.append("No kafka timestamp supplied when mode is [")
                        .append(StartupMode.TIMESTAMP.name())
                        .append("];\n");
            }else if(StartupMode.SPECIFIC_OFFSETS.equals(format.mode)){
                try {
                    KafkaUtil.parseSpecificOffsetsString(format.topic, format.offset);
                }catch (IllegalArgumentException e){
                    sb.append(e.getMessage()).append("\n");
                }
            }
        }else if(StartupMode.UNKNOWN.equals(format.mode)){
            sb.append("parameter [mode] config error, the value of mode must in [group-offsets, earliest-offset, latest-offset, timestamp, specific-offsets]\n");
        }

        if(!KafkaVersion.kafka09.equals(format.getKafkaVersion())){
            if (!format.consumerSettings.containsKey(KafkaConfigKeys.BOOTSTRAP_SERVERS)) {
                sb.append("parameter [")
                        .append(KafkaConfigKeys.BOOTSTRAP_SERVERS)
                        .append("] must set in consumerSettings;\n");
            }
        }

        if(sb.length() > 0){
            throw new IllegalArgumentException(sb.toString());
        }
    }
}
