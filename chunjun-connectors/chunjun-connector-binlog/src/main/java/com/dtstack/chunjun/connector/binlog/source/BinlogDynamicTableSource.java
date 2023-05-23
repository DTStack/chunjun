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

package com.dtstack.chunjun.connector.binlog.source;

import com.dtstack.chunjun.connector.binlog.config.BinlogConfig;
import com.dtstack.chunjun.connector.binlog.converter.BinlogSqlConverter;
import com.dtstack.chunjun.connector.binlog.format.TimestampFormat;
import com.dtstack.chunjun.connector.binlog.inputformat.BinlogInputFormatBuilder;
import com.dtstack.chunjun.source.DtInputFormatSourceFunction;
import com.dtstack.chunjun.table.connector.source.ParallelSourceFunctionProvider;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;

public class BinlogDynamicTableSource implements ScanTableSource {
    private final ResolvedSchema schema;
    private final DataType dataType;
    private final BinlogConfig binlogConfig;
    private final TimestampFormat timestampFormat;

    public BinlogDynamicTableSource(
            ResolvedSchema schema,
            BinlogConfig binlogConfig,
            TimestampFormat timestampFormat,
            DataType dataType) {
        this.schema = schema;
        this.binlogConfig = binlogConfig;
        this.timestampFormat = timestampFormat;
        this.dataType = dataType;
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        TypeInformation<RowData> typeInformation = InternalTypeInfo.of(dataType.getLogicalType());

        BinlogInputFormatBuilder builder = new BinlogInputFormatBuilder();
        builder.setBinlogConf(binlogConfig);
        builder.setRowConverter(
                new BinlogSqlConverter(
                        InternalTypeInfo.of(dataType.getLogicalType()).toRowType(),
                        this.timestampFormat));

        return ParallelSourceFunctionProvider.of(
                new DtInputFormatSourceFunction<>(builder.finish(), typeInformation), false, 1);
    }

    @Override
    public DynamicTableSource copy() {
        return new BinlogDynamicTableSource(schema, binlogConfig, timestampFormat, dataType);
    }

    @Override
    public String asSummaryString() {
        return "BinlogDynamicTableSource:";
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
