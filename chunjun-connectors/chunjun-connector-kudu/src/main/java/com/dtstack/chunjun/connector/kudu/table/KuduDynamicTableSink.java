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

package com.dtstack.chunjun.connector.kudu.table;

import com.dtstack.chunjun.connector.kudu.config.KuduSinkConfig;
import com.dtstack.chunjun.connector.kudu.converter.KuduSqlConverter;
import com.dtstack.chunjun.connector.kudu.sink.KuduOutputFormatBuilder;
import com.dtstack.chunjun.sink.DtOutputFormatSinkFunction;

import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

public class KuduDynamicTableSink implements DynamicTableSink {

    private static final String IDENTIFIER = "Kudu";

    private final ResolvedSchema tableSchema;

    private final KuduSinkConfig sinkConfig;

    public KuduDynamicTableSink(KuduSinkConfig sinkConfig, ResolvedSchema tableSchema) {
        this.tableSchema = tableSchema;
        this.sinkConfig = sinkConfig;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        // TODO upsert ? update ? append ?
        return ChangelogMode.insertOnly();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        KuduOutputFormatBuilder builder = new KuduOutputFormatBuilder();

        LogicalType logicalType = tableSchema.toPhysicalRowDataType().getLogicalType();

        builder.setSinkConfig(sinkConfig);
        builder.setRowConverter(
                new KuduSqlConverter((RowType) logicalType, tableSchema.getColumnNames()));

        return SinkFunctionProvider.of(
                new DtOutputFormatSinkFunction<>(builder.finish()), sinkConfig.getParallelism());
    }

    @Override
    public DynamicTableSink copy() {
        return new KuduDynamicTableSink(sinkConfig, tableSchema);
    }

    @Override
    public String asSummaryString() {
        return IDENTIFIER;
    }
}
