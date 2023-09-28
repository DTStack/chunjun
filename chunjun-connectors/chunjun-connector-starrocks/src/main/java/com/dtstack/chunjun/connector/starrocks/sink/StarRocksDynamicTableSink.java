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

package com.dtstack.chunjun.connector.starrocks.sink;

import com.dtstack.chunjun.config.FieldConfig;
import com.dtstack.chunjun.config.TypeConfig;
import com.dtstack.chunjun.connector.starrocks.config.StarRocksConfig;
import com.dtstack.chunjun.connector.starrocks.converter.StarRocksSqlConverter;
import com.dtstack.chunjun.sink.DtOutputFormatSinkFunction;

import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.types.RowKind;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkState;

public class StarRocksDynamicTableSink implements DynamicTableSink {
    private final StarRocksConfig sinkConfig;
    private final ResolvedSchema resolvedSchema;

    public StarRocksDynamicTableSink(StarRocksConfig sinkConfig, ResolvedSchema resolvedSchema) {
        this.sinkConfig = sinkConfig;
        this.resolvedSchema = resolvedSchema;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        validatePrimaryKey(requestedMode);
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.DELETE)
                .addContainedKind(RowKind.UPDATE_BEFORE)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .build();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        StarRocksOutputFormatBuilder builder =
                new StarRocksOutputFormatBuilder(new StarRocksOutputFormat());

        sinkConfig.setColumn(getFieldConfigFromSchema());
        builder.setRowConverter(
                new StarRocksSqlConverter(
                        InternalTypeInfo.of(resolvedSchema.toPhysicalRowDataType().getLogicalType())
                                .toRowType(),
                        Arrays.asList(resolvedSchema.getColumnNames().toArray(new String[0]))));
        builder.setStarRocksConf(sinkConfig);

        builder.checkFormat();
        return SinkFunctionProvider.of(
                new DtOutputFormatSinkFunction<>(builder.finish()), sinkConfig.getParallelism());
    }

    @Override
    public DynamicTableSink copy() {
        return new StarRocksDynamicTableSink(sinkConfig, resolvedSchema);
    }

    @Override
    public String asSummaryString() {
        return "StarRocks Sink";
    }

    private List<FieldConfig> getFieldConfigFromSchema() {

        return resolvedSchema.getColumns().stream()
                .map(
                        tableColumn -> {
                            FieldConfig fieldConfig = new FieldConfig();
                            fieldConfig.setName(tableColumn.getName());
                            fieldConfig.setType(
                                    TypeConfig.fromString(
                                            tableColumn
                                                    .getDataType()
                                                    .getLogicalType()
                                                    .asSummaryString()));
                            return fieldConfig;
                        })
                .collect(Collectors.toList());
    }

    private void validatePrimaryKey(ChangelogMode requestedMode) {
        checkState(
                ChangelogMode.insertOnly().equals(requestedMode)
                        || resolvedSchema.getPrimaryKey().isPresent(),
                "please declare primary key for sink table when query contains update/delete record.");
    }
}
