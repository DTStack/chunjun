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

package com.dtstack.chunjun.connector.hive3.sink;

import com.dtstack.chunjun.config.FieldConfig;
import com.dtstack.chunjun.config.SyncConfig;
import com.dtstack.chunjun.connector.hive3.config.HdfsConfig;
import com.dtstack.chunjun.connector.hive3.converter.HdfsRawTypeMapper;
import com.dtstack.chunjun.connector.hive3.util.Hive3Util;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.converter.RawTypeMapper;
import com.dtstack.chunjun.sink.SinkFactory;
import com.dtstack.chunjun.util.GsonUtil;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.data.RowData;

import org.apache.commons.collections4.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class Hive3SinkFactory extends SinkFactory {
    private final HdfsConfig hdfsConfig;

    public Hive3SinkFactory(SyncConfig syncConfig) {
        super(syncConfig);
        hdfsConfig =
                GsonUtil.GSON.fromJson(
                        GsonUtil.GSON.toJson(syncConfig.getWriter().getParameter()),
                        HdfsConfig.class);
        hdfsConfig.setColumn(syncConfig.getWriter().getFieldList());
        hdfsConfig.setFieldDelimiter(
                com.dtstack.chunjun.util.StringUtil.convertRegularExpr(
                        hdfsConfig.getFieldDelimiter()));

        initFullColumnMessage(hdfsConfig);
        super.initCommonConf(hdfsConfig);
    }

    @Override
    public RawTypeMapper getRawTypeMapper() {
        return HdfsRawTypeMapper::apply;
    }

    @Override
    public DataStreamSink<RowData> createSink(DataStream<RowData> dataSet) {
        Hive3OutputFormatBuilder builder =
                Hive3OutputFormatBuilder.newBuild(hdfsConfig.getFileType());
        builder.setHdfsConf(hdfsConfig);
        AbstractRowConverter rowConverter =
                Hive3Util.createRowConverter(
                        useAbstractBaseColumn,
                        hdfsConfig.getFileType(),
                        hdfsConfig.getColumn(),
                        getRawTypeMapper(),
                        hdfsConfig);

        builder.setRowConverter(rowConverter, useAbstractBaseColumn);
        return createOutput(dataSet, builder.finish());
    }

    public void initFullColumnMessage(HdfsConfig hdfsConfig) {
        List<String> fullColumnNameList = hdfsConfig.getFullColumnName();
        List<String> fullColumnTypeList = hdfsConfig.getFullColumnType();
        if (CollectionUtils.isEmpty(fullColumnNameList)) {
            fullColumnNameList =
                    hdfsConfig.getColumn().stream()
                            .map(FieldConfig::getName)
                            .collect(Collectors.toList());
        }
        if (CollectionUtils.isEmpty(fullColumnTypeList)) {
            fullColumnTypeList = new ArrayList<>(fullColumnNameList.size());
        }
        int[] fullColumnIndexes = new int[fullColumnNameList.size()];
        List<FieldConfig> fieldConfList = hdfsConfig.getColumn();
        for (int i = 0; i < fullColumnNameList.size(); i++) {
            String columnName = fullColumnNameList.get(i);
            int j = 0;
            for (; j < fieldConfList.size(); j++) {
                FieldConfig fieldConfig = fieldConfList.get(j);
                if (columnName.equalsIgnoreCase(fieldConfig.getName())) {
                    fullColumnTypeList.add(fieldConfig.getType().getType());
                    break;
                }
            }
            if (j == fieldConfList.size()) {
                j = -1;
            }
            fullColumnIndexes[i] = j;
        }
        hdfsConfig.setFullColumnName(fullColumnNameList);
        hdfsConfig.setFullColumnType(fullColumnTypeList);
        hdfsConfig.setFullColumnIndexes(fullColumnIndexes);
    }
}
