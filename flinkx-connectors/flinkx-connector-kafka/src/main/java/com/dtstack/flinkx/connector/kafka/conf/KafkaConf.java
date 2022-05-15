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
package com.dtstack.flinkx.connector.kafka.conf;

import com.dtstack.flinkx.conf.FlinkxCommonConf;
import com.dtstack.flinkx.connector.kafka.enums.StartupMode;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

/**
 * Reason: Date: 2018/09/18 Company: www.dtstack.com
 *
 * @author sishu.yss
 */
public class KafkaConf extends FlinkxCommonConf {

    /** source 读取数据的格式 */
    private String codec = "text";
    /** kafka topic */
    private String topic;
    /** 默认需要一个groupId */
    private String groupId = UUID.randomUUID().toString().replace("-", "");
    /** kafka启动模式 */
    private StartupMode mode = StartupMode.GROUP_OFFSETS;
    /** 消费位置,partition:0,offset:42;partition:1,offset:300 */
    private String offset = "";
    /** 当消费位置为TIMESTAMP时该参数设置才有效 */
    private long timestamp = -1L;
    /** kafka其他原生参数 */
    private Map<String, String> consumerSettings;
    /** kafka其他原生参数 */
    private Map<String, String> producerSettings;
    /** 字段映射配置。从reader插件传递到writer插件的的数据只包含其value属性，配置该参数后可将其还原成键值对类型json字符串输出。 */
    private List<String> tableFields;
    /** 是否强制有序，如果是则并行度只能为1 */
    private boolean dataCompelOrder;
    /** kafka sink分区字段 */
    private List<String> partitionAssignColumns;

    private String deserialization = "default";

    private boolean pavingData;

    private boolean split;

    public String getCodec() {
        return codec;
    }

    public void setCodec(String codec) {
        this.codec = codec;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public StartupMode getMode() {
        return mode;
    }

    public void setMode(StartupMode mode) {
        this.mode = mode;
    }

    public String getOffset() {
        if (offset == null) {
            return offset;
        }
        return offset.toLowerCase(Locale.ENGLISH);
    }

    public void setOffset(String offset) {
        this.offset = offset;
        if (this.offset != null) {
            this.offset = this.offset.toLowerCase(Locale.ENGLISH);
        }
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public Map<String, String> getConsumerSettings() {
        return consumerSettings;
    }

    public void setConsumerSettings(Map<String, String> consumerSettings) {
        this.consumerSettings = consumerSettings;
    }

    public Map<String, String> getProducerSettings() {
        return producerSettings;
    }

    public void setProducerSettings(Map<String, String> producerSettings) {
        this.producerSettings = producerSettings;
    }

    public List<String> getTableFields() {
        return tableFields;
    }

    public void setTableFields(List<String> tableFields) {
        this.tableFields = tableFields;
    }

    public List<String> getPartitionAssignColumns() {
        return partitionAssignColumns;
    }

    public void setPartitionAssignColumns(List<String> partitionAssignColumns) {
        this.partitionAssignColumns = partitionAssignColumns;
    }

    public boolean isDataCompelOrder() {
        return dataCompelOrder;
    }

    public void setDataCompelOrder(boolean dataCompelOrder) {
        this.dataCompelOrder = dataCompelOrder;
    }

    public String getDeserialization() {
        return deserialization;
    }

    public void setDeserialization(String deserialization) {
        this.deserialization = deserialization;
    }

    public boolean isPavingData() {
        return pavingData;
    }

    public void setPavingData(boolean pavingData) {
        this.pavingData = pavingData;
    }

    public boolean isSplit() {
        return split;
    }

    public void setSplit(boolean split) {
        this.split = split;
    }

    @Override
    public String toString() {
        return "KafkaConf{"
                + ", codec='"
                + codec
                + '\''
                + ", topic='"
                + topic
                + '\''
                + ", groupId='"
                + groupId
                + '\''
                + ", mode="
                + mode
                + ", offset='"
                + offset
                + '\''
                + ", timestamp="
                + timestamp
                + ", consumerSettings="
                + consumerSettings
                + ", producerSettings="
                + producerSettings
                + ", dataCompelOrder="
                + dataCompelOrder
                + ", tableFields="
                + tableFields
                + ", partitionAssignColumns="
                + partitionAssignColumns
                + '}';
    }
}
