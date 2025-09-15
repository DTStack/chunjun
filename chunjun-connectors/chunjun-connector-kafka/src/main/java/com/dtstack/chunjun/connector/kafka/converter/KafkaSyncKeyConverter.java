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

package com.dtstack.chunjun.connector.kafka.converter;

import com.dtstack.chunjun.connector.kafka.conf.KafkaConfig;
import com.dtstack.chunjun.connector.kafka.sink.PartitionStrategy;
import com.dtstack.chunjun.element.ColumnRowData;
import com.dtstack.chunjun.element.column.MapColumn;
import com.dtstack.chunjun.util.MapUtil;

import org.apache.flink.table.data.RowData;
import org.apache.flink.util.CollectionUtil;

import org.apache.commons.lang3.StringUtils;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * @author wujuan
 * @create 2025-12-01 15:51
 * @description
 */
public class KafkaSyncKeyConverter extends KafkaSyncConverter {

    private final PartitionStrategy partitionStrategy;

    public KafkaSyncKeyConverter(KafkaConfig kafkaConf, List<String> keyTypeList) {
        super(kafkaConf, keyTypeList);
        this.partitionStrategy = PartitionStrategy.fromValue(kafkaConf.getPartitionStrategy());
    }

    public KafkaSyncKeyConverter(KafkaConfig kafkaConf) {
        this(kafkaConf, null);
    }

    @Override
    public byte[] toExternal(RowData rowData, byte[] output) throws Exception {

        Map<String, Object> map = getExternalMap(rowData);

        int arity = rowData.getArity();
        ColumnRowData row = (ColumnRowData) rowData;

        if (kafkaConfig.getTableFields() != null
                && kafkaConfig.getTableFields().size() >= arity
                && !(row.getField(0) instanceof MapColumn)) {
            // get partition key value
            if (!CollectionUtil.isNullOrEmpty(outList)) {
                Map<String, Object> keyPartitionMap = new LinkedHashMap<>(1);
                for (Map.Entry<String, Object> entry : map.entrySet()) {
                    if (outList.contains(entry.getKey())) {
                        keyPartitionMap.put(entry.getKey(), entry.getValue());
                    }
                }
                map = keyPartitionMap;
            }
        } else {
            //  CDC kafka key handler
            // partition.strategy  hash-by-key / all-to-zero
            if (partitionStrategy != null
                    && partitionStrategy.equals(PartitionStrategy.HASH_BY_KEY)) {
                Map<String, Object> keyPartitionMap = new LinkedHashMap<>(1);
                if (map.get("message") instanceof Map) {
                    // cdc 数据，折叠类型的数据
                    Map<String, Object> message = (Map<String, Object>) map.get("message");
                    String type = (String) message.get("type");
                    String primaryKey = (String) (message).get("primary_key");

                    if (StringUtils.isNotEmpty(primaryKey)) {
                        String[] primaryKeys = primaryKey.split(",");
                        switch (type) {
                            case "DELETE":
                                {
                                    Map<String, Object> before =
                                            (Map<String, Object>) message.get("before");
                                    for (String key : primaryKeys) {
                                        keyPartitionMap.put(key, before.get(key));
                                    }
                                    map = keyPartitionMap;
                                }
                                break;
                            case "INSERT":
                            case "UPDATE":
                                {
                                    Map<String, Object> after =
                                            (Map<String, Object>) message.get("after");
                                    for (String key : primaryKeys) {
                                        keyPartitionMap.put(key, after.get(key));
                                    }
                                    map = keyPartitionMap;
                                }
                                break;
                            default:
                        }
                    }

                } else {
                    // cdc 数据，paving data.  pavingData = true
                    String primaryKey = (String) map.get("primary_key");
                    String type = (String) map.get("type");
                    if (StringUtils.isNotEmpty(primaryKey)) {
                        String[] primaryKeys = primaryKey.split(",");
                        String prefix;
                        switch (type) {
                            case "INSERT":
                            case "UPDATE":
                                prefix = "after_";
                                break;
                            case "DELETE":
                            default:
                                prefix = "before_";
                                break;
                        }

                        for (String key : primaryKeys) {
                            keyPartitionMap.put(key, map.get(prefix + key));
                        }
                        map = keyPartitionMap;
                    }
                }
            }
        }
        return MapUtil.writeValueAsString(map).getBytes(StandardCharsets.UTF_8);
    }
}
