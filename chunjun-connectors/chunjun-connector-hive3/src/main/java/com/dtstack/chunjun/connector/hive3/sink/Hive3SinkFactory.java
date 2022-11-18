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

import com.dtstack.chunjun.config.FieldConf;
import com.dtstack.chunjun.config.SyncConf;
import com.dtstack.chunjun.connector.hive3.conf.HdfsConf;
import com.dtstack.chunjun.connector.hive3.converter.HdfsRawTypeConverter;
import com.dtstack.chunjun.connector.hive3.util.Hive3Util;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.converter.RawTypeConverter;
import com.dtstack.chunjun.sink.SinkFactory;
import com.dtstack.chunjun.util.GsonUtil;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.data.RowData;

import org.apache.commons.collections4.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/** @author liuliu 2022/3/23 */
public class Hive3SinkFactory extends SinkFactory {
    private final HdfsConf hdfsConf;

    public Hive3SinkFactory(SyncConf syncConf) {
        super(syncConf);
        hdfsConf =
                GsonUtil.GSON.fromJson(
                        GsonUtil.GSON.toJson(syncConf.getWriter().getParameter()), HdfsConf.class);
        hdfsConf.setColumn(syncConf.getWriter().getFieldList());
        hdfsConf.setFieldDelimiter(
                com.dtstack.chunjun.util.StringUtil.convertRegularExpr(
                        hdfsConf.getFieldDelimiter()));

        initFullColumnMessage(hdfsConf);
        super.initCommonConf(hdfsConf);
    }

    @Override
    public RawTypeConverter getRawTypeConverter() {
        return HdfsRawTypeConverter::apply;
    }

    @Override
    public DataStreamSink<RowData> createSink(DataStream<RowData> dataSet) {
        Hive3OutputFormatBuilder builder =
                Hive3OutputFormatBuilder.newBuild(hdfsConf.getFileType());
        builder.setHdfsConf(hdfsConf);
        AbstractRowConverter rowConverter =
                Hive3Util.createRowConverter(
                        useAbstractBaseColumn,
                        hdfsConf.getFileType(),
                        hdfsConf.getColumn(),
                        getRawTypeConverter(),
                        hdfsConf);

        builder.setRowConverter(rowConverter, useAbstractBaseColumn);
        return createOutput(dataSet, builder.finish());
    }

    public void initFullColumnMessage(HdfsConf hdfsConf) {
        List<String> fullColumnNameList = hdfsConf.getFullColumnName();
        List<String> fullColumnTypeList = hdfsConf.getFullColumnType();
        if (CollectionUtils.isEmpty(fullColumnNameList)) {
            fullColumnNameList =
                    hdfsConf.getColumn().stream()
                            .map(FieldConf::getName)
                            .collect(Collectors.toList());
        }
        if (CollectionUtils.isEmpty(fullColumnTypeList)) {
            fullColumnTypeList = new ArrayList<>(fullColumnNameList.size());
        }
        int[] fullColumnIndexes = new int[fullColumnNameList.size()];
        List<FieldConf> fieldConfList = hdfsConf.getColumn();
        for (int i = 0; i < fullColumnNameList.size(); i++) {
            String columnName = fullColumnNameList.get(i);
            int j = 0;
            for (; j < fieldConfList.size(); j++) {
                FieldConf fieldConf = fieldConfList.get(j);
                if (columnName.equalsIgnoreCase(fieldConf.getName())) {
                    fullColumnTypeList.add(fieldConf.getType());
                    break;
                }
            }
            if (j == fieldConfList.size()) {
                j = -1;
            }
            fullColumnIndexes[i] = j;
        }
        hdfsConf.setFullColumnName(fullColumnNameList);
        hdfsConf.setFullColumnType(fullColumnTypeList);
        hdfsConf.setFullColumnIndexes(fullColumnIndexes);
    }
}
