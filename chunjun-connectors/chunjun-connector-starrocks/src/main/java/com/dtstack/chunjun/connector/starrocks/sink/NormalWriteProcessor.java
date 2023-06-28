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
import com.dtstack.chunjun.connector.starrocks.streamload.StarRocksSinkBufferEntity;
import com.dtstack.chunjun.connector.starrocks.streamload.StreamLoadManager;
import com.dtstack.chunjun.converter.AbstractRowConverter;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NormalWriteProcessor extends StarRocksWriteProcessor {

    private final AbstractRowConverter<Object[], Object[], Map<String, Object>, LogicalType>
            rowConverter;
    private final List<String> columnNameList;

    public NormalWriteProcessor(
            AbstractRowConverter<Object[], Object[], Map<String, Object>, LogicalType> converter,
            StreamLoadManager streamLoadManager,
            StarRocksConfig starRocksConfig,
            List<String> columnNameList) {
        super(streamLoadManager, starRocksConfig);
        this.rowConverter = converter;
        this.columnNameList = columnNameList;

        streamLoadManager.validateTableStructure(
                new StarRocksSinkBufferEntity(
                        starRocksConfig.getDatabase(), starRocksConfig.getTable(), columnNameList));
    }

    @Override
    public void write(List<RowData> rowDataList) throws Exception {
        String identify =
                String.format("%s.%s", starRocksConfig.getDatabase(), starRocksConfig.getTable());
        List<Map<String, Object>> values = new ArrayList<>(rowDataList.size());
        for (RowData rowData : rowDataList) {
            values.add(rowConverter.toExternal(rowData, new HashMap<>(columnNameList.size())));
        }
        streamLoadManager.write(identify, columnNameList, values, false);
    }
}
