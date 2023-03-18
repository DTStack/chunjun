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
package com.dtstack.chunjun.connector.kafka.conf;

import com.dtstack.chunjun.config.CommonConfig;
import com.dtstack.chunjun.config.FieldConfig;
import com.dtstack.chunjun.connector.kafka.enums.StartupMode;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

@Getter
@Setter
@ToString
public class KafkaConfig extends CommonConfig {

    /** source 读取数据的格式 */
    private String codec = "text";

    /** codec为json的时候 使用加上message作为key* */
    private boolean addMessage = true;
    /** kafka topic */
    private String topic;
    /** kafka topics */
    private List<String> topics;
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

    /** deserialization的配置信息，每个deserialization的配置信息是不一样的 * */
    private Map<String, Object> deserializationProperties = new HashMap<>();

    private boolean pavingData;

    private boolean split;

    private Map<String, List<FieldConfig>> tableSchema;

    public List<String> filter;

    /** topicSchema里指定的字段不在kafka数据时 是否需要用null代替 */
    private boolean nullReplaceNotExistsField = true;

    public String getOffset() {
        if (offset == null) {
            return null;
        }
        return offset.toLowerCase(Locale.ENGLISH);
    }

    public void setOffset(String offset) {
        this.offset = offset;
        if (this.offset != null) {
            this.offset = this.offset.toLowerCase(Locale.ENGLISH);
        }
    }
}
