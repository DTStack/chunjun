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

package com.dtstack.chunjun.connector.s3.source;

import com.dtstack.chunjun.config.FieldConfig;
import com.dtstack.chunjun.config.RestoreConfig;
import com.dtstack.chunjun.config.TypeConfig;
import com.dtstack.chunjun.connector.s3.config.S3Config;
import com.dtstack.chunjun.connector.s3.converter.S3SqlConverter;
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
import org.apache.flink.table.types.logical.LogicalType;

import java.util.ArrayList;
import java.util.List;

public class S3DynamicTableSource implements ScanTableSource {
    private final ResolvedSchema schema;
    private final S3Config s3Config;

    public S3DynamicTableSource(ResolvedSchema schema, S3Config s3Config) {
        this.schema = schema;
        this.s3Config = s3Config;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        LogicalType logicalType = schema.toPhysicalRowDataType().getLogicalType();
        TypeInformation<RowData> typeInformation = InternalTypeInfo.of(logicalType);

        List<Column> columns = schema.getColumns();
        List<FieldConfig> columnList = new ArrayList<>(columns.size());
        for (int i = 0; i < columns.size(); i++) {
            Column column = columns.get(i);
            FieldConfig field = new FieldConfig();
            field.setName(column.getName());
            field.setType(
                    TypeConfig.fromString(column.getDataType().getLogicalType().asSummaryString()));
            int index =
                    s3Config.getExcelFormatConfig().getColumnIndex() != null
                            ? s3Config.getExcelFormatConfig()
                                    .getColumnIndex()
                                    .get(columns.indexOf(column))
                            : columns.indexOf(column);
            field.setIndex(index);
            columnList.add(field);
        }
        s3Config.setColumn(columnList);
        S3InputFormatBuilder builder = new S3InputFormatBuilder(new S3InputFormat());
        builder.setRestoreConf(new RestoreConfig());
        builder.setRowConverter(
                new S3SqlConverter(InternalTypeInfo.of(logicalType).toRowType(), s3Config));
        builder.setS3Conf(s3Config);
        return ParallelSourceFunctionProvider.of(
                new DtInputFormatSourceFunction<>(builder.finish(), typeInformation), false, null);
    }

    @Override
    public DynamicTableSource copy() {
        return new S3DynamicTableSource(this.schema, this.s3Config);
    }

    @Override
    public String asSummaryString() {
        return "S3DynamicTableSource";
    }
}
