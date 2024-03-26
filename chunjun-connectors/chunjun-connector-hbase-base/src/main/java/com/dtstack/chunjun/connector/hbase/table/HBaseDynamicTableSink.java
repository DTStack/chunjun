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
package com.dtstack.chunjun.connector.hbase.table;

import com.dtstack.chunjun.connector.hbase.HBaseTableSchema;
import com.dtstack.chunjun.connector.hbase.config.HBaseConfig;
import com.dtstack.chunjun.connector.hbase.converter.HBaseSqlConverter;
import com.dtstack.chunjun.connector.hbase.sink.HBaseOutputFormatBuilder;
import com.dtstack.chunjun.sink.DtOutputFormatSinkFunction;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;

public class HBaseDynamicTableSink implements DynamicTableSink {

    private final HBaseConfig config;
    private final TableSchema tableSchema;
    private final HBaseTableSchema hbaseSchema;

    public HBaseDynamicTableSink(
            HBaseConfig config, TableSchema tableSchema, HBaseTableSchema hbaseSchema) {
        this.config = config;
        this.tableSchema = tableSchema;
        this.hbaseSchema = hbaseSchema;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return requestedMode;
    }

    @Override
    public SinkFunctionProvider getSinkRuntimeProvider(Context context) {

        HBaseOutputFormatBuilder builder = new HBaseOutputFormatBuilder();
        builder.setHbaseConfig(config.getHbaseConfig());
        builder.setTableName(config.getTable());
        builder.setWriteBufferSize(config.getWriteBufferSize());
        HBaseSqlConverter hbaseSqlConverter = new HBaseSqlConverter(hbaseSchema, config);
        builder.setRowConverter(hbaseSqlConverter);
        builder.setConfig(config);
        return SinkFunctionProvider.of(
                new DtOutputFormatSinkFunction<>(builder.finish()), config.getParallelism());
    }

    @Override
    public DynamicTableSink copy() {
        return new HBaseDynamicTableSink(config, tableSchema, hbaseSchema);
    }

    @Override
    public String asSummaryString() {
        return "HbaseDynamicTableSink";
    }
}
