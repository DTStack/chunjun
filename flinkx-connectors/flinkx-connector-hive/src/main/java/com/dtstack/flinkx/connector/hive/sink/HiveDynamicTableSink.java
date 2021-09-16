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
package com.dtstack.flinkx.connector.hive.sink;

import com.dtstack.flinkx.connector.hive.conf.HiveConf;
import com.dtstack.flinkx.connector.hive.util.HiveUtil;
import com.dtstack.flinkx.sink.DtOutputFormatSinkFunction;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;

/**
 * Date: 2021/06/22 Company: www.dtstack.com
 *
 * @author tudou
 */
public class HiveDynamicTableSink implements DynamicTableSink {

    private final HiveConf hiveConf;
    private final TableSchema tableSchema;

    public HiveDynamicTableSink(HiveConf hiveConf, TableSchema tableSchema) {
        this.hiveConf = hiveConf;
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
        hiveConf.setTableInfos(
                HiveUtil.formatHiveTableInfo(
                        hiveConf.getTablesColumn(),
                        hiveConf.getPartition(),
                        hiveConf.getFieldDelimiter(),
                        hiveConf.getFileType()));
        builder.setHiveConf(hiveConf);
        return SinkFunctionProvider.of(
                new DtOutputFormatSinkFunction(builder.finish()), hiveConf.getParallelism());
    }

    @Override
    public DynamicTableSink copy() {
        return new HiveDynamicTableSink(hiveConf, tableSchema);
    }

    @Override
    public String asSummaryString() {
        return "HiveDynamicTableSink";
    }
}
