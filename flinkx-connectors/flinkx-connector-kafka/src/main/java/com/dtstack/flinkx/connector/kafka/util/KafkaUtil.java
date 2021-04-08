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
package com.dtstack.flinkx.connector.kafka.util;

import com.dtstack.flinkx.constants.ConstantValue;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Date: 2020/12/31
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public class KafkaUtil {

    /**
     * 解析kafka offset字符串
     * @param topic
     * @param offsetString
     * @return
     * @throws IllegalArgumentException
     */
    public static Map<KafkaTopicPartition, Long> parseSpecificOffsetsString(String topic, String offsetString) throws IllegalArgumentException{
        final String[] pairs = offsetString.split(ConstantValue.SEMICOLON_SYMBOL);
        final String validationExceptionMessage = "Invalid properties [offset] should follow the format 'partition:0,offset:42;partition:1,offset:300', but is '" + offsetString + "';";

        if (pairs.length == 0) {
            throw new IllegalArgumentException(validationExceptionMessage);
        }

        Map<KafkaTopicPartition, Long> map = new HashMap<>();
        for (String pair : pairs) {
            if (null == pair || pair.length() == 0 || !pair.contains(ConstantValue.COMMA_SYMBOL)) {
                throw new IllegalArgumentException(validationExceptionMessage);
            }

            final String[] kv = pair.split(ConstantValue.COMMA_SYMBOL);
            if (kv.length != 2 ||
                    !kv[0].startsWith("partition:") ||
                    !kv[1].startsWith("offset:")) {
                throw new IllegalArgumentException(validationExceptionMessage);
            }

            String partitionValue = kv[0].substring(kv[0].indexOf(ConstantValue.COLON_SYMBOL) + 1);
            String offsetValue = kv[1].substring(kv[1].indexOf(ConstantValue.COLON_SYMBOL) + 1);
            try {
                final int partition = Integer.parseInt(partitionValue);
                final Long offset = Long.valueOf(offsetValue);
                map.put(new KafkaTopicPartition(topic, partition), offset);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(validationExceptionMessage, e);
            }
        }
        return map;
    }

    /**
     * 获取partition key
     * @param partitionKeys
     * @return
     */
    public static String[] getPartitionKeys(String partitionKeys) {
        if (StringUtils.isNotBlank(partitionKeys)) {
            String[] keys = StringUtils.split(partitionKeys, ",");
            return Arrays.stream(keys)
                    .map(String::trim)
                    .toArray(String[]::new);
        }
        return null;
    }
}
