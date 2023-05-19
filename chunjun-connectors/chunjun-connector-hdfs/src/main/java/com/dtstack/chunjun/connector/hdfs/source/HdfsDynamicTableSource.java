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
package com.dtstack.chunjun.connector.hdfs.source;

import com.dtstack.chunjun.config.FieldConfig;
import com.dtstack.chunjun.config.TypeConfig;
import com.dtstack.chunjun.connector.hdfs.config.HdfsConfig;
import com.dtstack.chunjun.connector.hdfs.converter.HdfsOrcSqlConverter;
import com.dtstack.chunjun.connector.hdfs.converter.HdfsParquetSqlConverter;
import com.dtstack.chunjun.connector.hdfs.converter.HdfsTextSqlConverter;
import com.dtstack.chunjun.connector.hdfs.enums.FileType;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.source.DtInputFormatSourceFunction;
import com.dtstack.chunjun.table.connector.source.ParallelSourceFunctionProvider;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;

import java.util.ArrayList;
import java.util.List;

public class HdfsDynamicTableSource implements ScanTableSource {

    private final HdfsConfig hdfsConfig;
    private final ResolvedSchema tableSchema;
    private final List<String> partitionKeyList;

    public HdfsDynamicTableSource(HdfsConfig hdfsConfig, ResolvedSchema tableSchema) {
        this(hdfsConfig, tableSchema, new ArrayList<>());
    }

    public HdfsDynamicTableSource(
            HdfsConfig hdfsConfig, ResolvedSchema tableSchema, List<String> partitionKeyList) {
        this.hdfsConfig = hdfsConfig;
        this.tableSchema = tableSchema;
        this.partitionKeyList = partitionKeyList;
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        TypeInformation<RowData> typeInformation =
                InternalTypeInfo.of(tableSchema.toPhysicalRowDataType().getLogicalType());
        List<Column> columns = tableSchema.getColumns();
        List<FieldConfig> columnList = new ArrayList<>(columns.size());
        for (int i = 0; i < columns.size(); i++) {
            Column column = columns.get(i);
            String fieldName = column.getName();
            FieldConfig field = new FieldConfig();
            field.setName(fieldName);
            field.setType(
                    TypeConfig.fromString(column.getDataType().getLogicalType().asSummaryString()));
            field.setIndex(i);
            if (partitionKeyList.contains(fieldName)) {
                field.setIsPart(true);
            }
            columnList.add(field);
        }
        hdfsConfig.setColumn(columnList);
        HdfsInputFormatBuilder builder = HdfsInputFormatBuilder.newBuild(hdfsConfig.getFileType());
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
                                InternalTypeInfo.of(
                                                tableSchema
                                                        .toPhysicalRowDataType()
                                                        .getLogicalType())
                                        .toRowType());
        }
        builder.setRowConverter(rowConverter);
        return ParallelSourceFunctionProvider.of(
                new DtInputFormatSourceFunction<>(builder.finish(), typeInformation),
                false,
                hdfsConfig.getParallelism());
    }

    @Override
    public DynamicTableSource copy() {
        return new HdfsDynamicTableSource(this.hdfsConfig, this.tableSchema);
    }

    @Override
    public String asSummaryString() {
        return "HdfsDynamicTableSource:";
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }
}
