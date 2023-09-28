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

package com.dtstack.chunjun.connector.starrocks.sink;

import com.dtstack.chunjun.connector.starrocks.config.StarRocksConfig;
import com.dtstack.chunjun.connector.starrocks.streamload.StreamLoadManager;
import com.dtstack.chunjun.element.ColumnRowData;

import org.apache.flink.table.data.RowData;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MappedWriteProcessor extends StarRocksWriteProcessor {
    private final Set<String> metaHeader =
            Stream.of("schema", "table", "type", "opTime", "ts", "scn")
                    .collect(Collectors.toCollection(HashSet::new));

    public MappedWriteProcessor(
            StreamLoadManager streamLoadManager, StarRocksConfig starRocksConfig) {
        super(streamLoadManager, starRocksConfig);
    }

    @Override
    public void write(List<RowData> rowDataList) throws Exception {
        Map<String, List<Map<String, Object>>> identifyXValueMap = new HashMap<>();
        for (RowData data : rowDataList) {
            ColumnRowData rowData = (ColumnRowData) data;
            Map<String, Integer> headerInfo = rowData.getHeaderInfo();
            String schema = starRocksConfig.getDatabase();
            String table = starRocksConfig.getTable();
            Map<String, Object> value = new HashMap<>();
            for (Map.Entry<String, Integer> entry : headerInfo.entrySet()) {
                String key = entry.getKey();
                int index = entry.getValue();
                switch (key) {
                    case "schema":
                        schema = rowData.getField(index).asString();
                    case "table":
                        table = rowData.getField(index).asString();
                    default:
                        if (!metaHeader.contains(key)) {
                            value.put(key, rowData.getField(index).asString());
                        }
                }
            }
            String identify = String.format("%s.%s", schema, table);
            List<Map<String, Object>> valueList =
                    identifyXValueMap.computeIfAbsent(identify, key -> new ArrayList<>());
            valueList.add(value);
        }
        for (Map.Entry<String, List<Map<String, Object>>> entry : identifyXValueMap.entrySet()) {
            streamLoadManager.write(entry.getKey(), null, entry.getValue(), true);
        }
    }
}
