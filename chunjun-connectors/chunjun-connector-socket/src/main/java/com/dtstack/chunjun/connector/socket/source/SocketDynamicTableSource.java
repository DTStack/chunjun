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

package com.dtstack.chunjun.connector.socket.source;

import com.dtstack.chunjun.connector.socket.converter.SocketSqlConverter;
import com.dtstack.chunjun.connector.socket.entity.SocketConfig;
import com.dtstack.chunjun.connector.socket.inputformat.SocketInputFormatBuilder;
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

public class SocketDynamicTableSource implements ScanTableSource {

    private final ResolvedSchema schema;
    private final SocketConfig socketConfig;
    private final DataType physicalRowDataType;

    public SocketDynamicTableSource(
            ResolvedSchema schema, SocketConfig socketConfig, DataType physicalRowDataType) {
        this.schema = schema;
        this.socketConfig = socketConfig;
        this.physicalRowDataType = physicalRowDataType;
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        final TypeInformation<RowData> typeInformation =
                InternalTypeInfo.of(physicalRowDataType.getLogicalType());
        SocketInputFormatBuilder builder = new SocketInputFormatBuilder();
        builder.setRowConverter(
                new SocketSqlConverter(
                        InternalTypeInfo.of(physicalRowDataType.getLogicalType()).toRowType()));
        builder.setSocketConfig(socketConfig);

        return ParallelSourceFunctionProvider.of(
                new DtInputFormatSourceFunction<>(builder.finish(), typeInformation),
                false,
                socketConfig.getParallelism());
    }

    @Override
    public DynamicTableSource copy() {
        return new SocketDynamicTableSource(schema, socketConfig, physicalRowDataType);
    }

    @Override
    public String asSummaryString() {
        return "SocketDynamicTableSource:";
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }
}
