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

package com.dtstack.chunjun.connector.nebula.sink;

import com.dtstack.chunjun.connector.nebula.config.NebulaConfig;
import com.dtstack.chunjun.connector.nebula.converter.NebulaSqlConverter;
import com.dtstack.chunjun.sink.DtOutputFormatSinkFunction;

import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

public class NebulaDynamicTableSink implements DynamicTableSink {

    protected final ResolvedSchema tableSchema;

    protected final NebulaConfig nebulaConfig;

    protected final NebulaOutputFormatBuilder builder;

    public NebulaDynamicTableSink(
            ResolvedSchema tableSchema,
            NebulaConfig nebulaConfig,
            NebulaOutputFormatBuilder builder) {
        this.tableSchema = tableSchema;
        this.nebulaConfig = nebulaConfig;
        this.builder = builder;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .build();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        final RowType rowType =
                InternalTypeInfo.of(tableSchema.toPhysicalRowDataType().getLogicalType())
                        .toRowType();
        builder.setNebulaConfig(nebulaConfig);
        builder.setRowConverter(new NebulaSqlConverter(rowType));
        return SinkFunctionProvider.of(
                new DtOutputFormatSinkFunction<>(builder.finish()), nebulaConfig.getWriteTasks());
    }

    @Override
    public DynamicTableSink copy() {
        return new NebulaDynamicTableSink(tableSchema, nebulaConfig, builder);
    }

    @Override
    public String asSummaryString() {
        return "nebula";
    }
}
