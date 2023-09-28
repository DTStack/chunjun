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

package com.dtstack.chunjun.connector.sqlservercdc.source;

import com.dtstack.chunjun.connector.sqlservercdc.config.SqlServerCdcConfig;
import com.dtstack.chunjun.connector.sqlservercdc.convert.SqlServerCdcSqlConverter;
import com.dtstack.chunjun.connector.sqlservercdc.format.TimestampFormat;
import com.dtstack.chunjun.connector.sqlservercdc.inputFormat.SqlServerCdcInputFormatBuilder;
import com.dtstack.chunjun.source.DtInputFormatSourceFunction;
import com.dtstack.chunjun.table.connector.source.ParallelSourceFunctionProvider;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.RowKind;

public class SqlServerCdcDynamicTableSource implements ScanTableSource {
    private final ResolvedSchema schema;
    private final SqlServerCdcConfig sqlserverCdcConfig;
    private final TimestampFormat timestampFormat;

    public SqlServerCdcDynamicTableSource(
            ResolvedSchema schema,
            SqlServerCdcConfig sqlserverCdcConfig,
            TimestampFormat timestampFormat) {
        this.schema = schema;
        this.sqlserverCdcConfig = sqlserverCdcConfig;
        this.timestampFormat = timestampFormat;
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        LogicalType logicalType = schema.toPhysicalRowDataType().getLogicalType();
        TypeInformation<RowData> typeInformation = InternalTypeInfo.of(logicalType);

        SqlServerCdcInputFormatBuilder builder = new SqlServerCdcInputFormatBuilder();
        builder.setSqlServerCdcConf(sqlserverCdcConfig);
        builder.setRowConverter(
                new SqlServerCdcSqlConverter(
                        InternalTypeInfo.of(logicalType).toRowType(), this.timestampFormat));

        return ParallelSourceFunctionProvider.of(
                new DtInputFormatSourceFunction<>(builder.finish(), typeInformation), false, 1);
    }

    @Override
    public DynamicTableSource copy() {
        return new SqlServerCdcDynamicTableSource(
                this.schema, this.sqlserverCdcConfig, this.timestampFormat);
    }

    @Override
    public String asSummaryString() {
        return "SqlServerCdcDynamicTableSource:";
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.UPDATE_BEFORE)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .addContainedKind(RowKind.DELETE)
                .build();
    }
}
