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
package com.dtstack.chunjun.connector.hdfs.sink;

import com.dtstack.chunjun.config.FieldConfig;
import com.dtstack.chunjun.config.TypeConfig;
import com.dtstack.chunjun.connector.hdfs.config.HdfsConfig;
import com.dtstack.chunjun.connector.hdfs.converter.HdfsOrcSqlConverter;
import com.dtstack.chunjun.connector.hdfs.converter.HdfsParquetSqlConverter;
import com.dtstack.chunjun.connector.hdfs.converter.HdfsTextSqlConverter;
import com.dtstack.chunjun.connector.hdfs.enums.FileType;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.sink.DtOutputFormatSinkFunction;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;

import java.util.ArrayList;
import java.util.List;

public class HdfsDynamicTableSink implements DynamicTableSink {

    protected final ResolvedSchema tableSchema;
    protected final HdfsConfig hdfsConfig;

    public HdfsDynamicTableSink(HdfsConfig hdfsConfig, ResolvedSchema tableSchema) {
        this.hdfsConfig = hdfsConfig;
        this.tableSchema = tableSchema;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return ChangelogMode.insertOnly();
    }

    @Override
    @SuppressWarnings("all")
    public SinkFunctionProvider getSinkRuntimeProvider(Context context) {
        final TypeInformation<?> typeInformation =
                InternalTypeInfo.of(tableSchema.toPhysicalRowDataType().getLogicalType());
        List<Column> columns = tableSchema.getColumns();
        List<FieldConfig> columnList = new ArrayList<>(columns.size());

        columns.forEach(
                column -> {
                    String name = column.getName();
                    TypeConfig type =
                            TypeConfig.fromString(
                                    column.getDataType().getLogicalType().asSummaryString());
                    FieldConfig field = new FieldConfig();
                    field.setName(name);
                    field.setType(type);
                    field.setIndex(columns.indexOf(column));
                    columnList.add(field);
                });
        hdfsConfig.setColumn(columnList);
        HdfsOutputFormatBuilder builder =
                HdfsOutputFormatBuilder.newBuild(hdfsConfig.getFileType());
        builder.setHdfsConf(hdfsConfig);
        AbstractRowConverter rowConverter;
        switch (FileType.getByName(hdfsConfig.getFileType())) {
            case ORC:
                rowConverter =
                        new HdfsOrcSqlConverter(
                                InternalTypeInfo.of(
                                                tableSchema
                                                        .toPhysicalRowDataType()
                                                        .getLogicalType())
                                        .toRowType());
                break;
            case PARQUET:
                rowConverter =
                        new HdfsParquetSqlConverter(
                                InternalTypeInfo.of(
                                                tableSchema
                                                        .toPhysicalRowDataType()
                                                        .getLogicalType())
                                        .toRowType());
                break;
            default:
                rowConverter =
                        new HdfsTextSqlConverter(
                                (InternalTypeInfo.of(
                                                tableSchema
                                                        .toPhysicalRowDataType()
                                                        .getLogicalType())
                                        .toRowType()));
        }
        builder.setRowConverter(rowConverter);
        return SinkFunctionProvider.of(
                new DtOutputFormatSinkFunction(builder.finish()), hdfsConfig.getParallelism());
    }

    @Override
    public DynamicTableSink copy() {
        return new HdfsDynamicTableSink(hdfsConfig, tableSchema);
    }

    @Override
    public String asSummaryString() {
        return "HdfsDynamicTableSink";
    }
}
