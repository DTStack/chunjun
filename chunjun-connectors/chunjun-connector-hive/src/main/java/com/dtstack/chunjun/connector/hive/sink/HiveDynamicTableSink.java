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
package com.dtstack.chunjun.connector.hive.sink;

import com.dtstack.chunjun.connector.hive.config.HiveConfig;
import com.dtstack.chunjun.connector.hive.util.HiveUtil;
import com.dtstack.chunjun.sink.DtOutputFormatSinkFunction;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;

public class HiveDynamicTableSink implements DynamicTableSink {

    private final HiveConfig config;
    private final TableSchema tableSchema;

    public HiveDynamicTableSink(HiveConfig config, TableSchema tableSchema) {
        this.config = config;
        this.tableSchema = tableSchema;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return ChangelogMode.insertOnly();
    }

    @Override
    @SuppressWarnings("all")
    public SinkFunctionProvider getSinkRuntimeProvider(Context context) {
        HiveOutputFormatBuilder builder = new HiveOutputFormatBuilder();
        config.setTableInfos(
                HiveUtil.formatHiveTableInfo(
                        config.getTablesColumn(),
                        config.getPartition(),
                        config.getFieldDelimiter(),
                        config.getFileType()));
        builder.setHiveConf(config);
        return SinkFunctionProvider.of(
                new DtOutputFormatSinkFunction(builder.finish()), config.getParallelism());
    }

    @Override
    public DynamicTableSink copy() {
        return new HiveDynamicTableSink(config, tableSchema);
    }

    @Override
    public String asSummaryString() {
        return "HiveDynamicTableSink";
    }
}
