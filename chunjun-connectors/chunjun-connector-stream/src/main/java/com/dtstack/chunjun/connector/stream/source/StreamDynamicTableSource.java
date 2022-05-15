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

package com.dtstack.chunjun.connector.stream.source;

import com.dtstack.chunjun.conf.FieldConf;
import com.dtstack.chunjun.connector.stream.conf.StreamConf;
import com.dtstack.chunjun.connector.stream.converter.StreamRowConverter;
import com.dtstack.chunjun.source.DtInputFormatSourceFunction;
import com.dtstack.chunjun.table.connector.source.ParallelSourceFunctionProvider;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @author chuixue
 * @create 2021-04-09 09:20
 * @description
 */
public class StreamDynamicTableSource implements ScanTableSource {

    private final TableSchema schema;
    private final StreamConf streamConf;

    public StreamDynamicTableSource(TableSchema schema, StreamConf streamConf) {
        this.schema = schema;
        this.streamConf = streamConf;
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        final RowType rowType = (RowType) schema.toRowDataType().getLogicalType();
        TypeInformation<RowData> typeInformation = InternalTypeInfo.of(rowType);

        List<FieldConf> fieldConfList =
                schema.getTableColumns().stream()
                        .map(
                                e -> {
                                    FieldConf fieldConf = new FieldConf();
                                    fieldConf.setName(e.getName());
                                    String name = e.getType().getConversionClass().getName();
                                    String[] fieldType = name.split("\\.");
                                    String type = fieldType[fieldType.length - 1];
                                    fieldConf.setType(type);
                                    return fieldConf;
                                })
                        .collect(Collectors.toList());

        streamConf.setColumn(fieldConfList);

        StreamInputFormatBuilder builder = new StreamInputFormatBuilder();
        builder.setRowConverter(new StreamRowConverter(rowType));
        builder.setStreamConf(streamConf);

        return ParallelSourceFunctionProvider.of(
                new DtInputFormatSourceFunction<>(builder.finish(), typeInformation), false, null);
    }

    @Override
    public DynamicTableSource copy() {
        return new StreamDynamicTableSource(this.schema, this.streamConf);
    }

    @Override
    public String asSummaryString() {
        return "StreamDynamicTableSource:";
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }
}
