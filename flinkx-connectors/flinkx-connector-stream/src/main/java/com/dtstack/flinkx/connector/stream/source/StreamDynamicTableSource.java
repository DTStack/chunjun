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

package com.dtstack.flinkx.connector.stream.source;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.AsyncTableFunctionProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.TableFunctionProvider;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.utils.TableSchemaUtils;

import com.dtstack.flinkx.connector.stream.lookup.StreamAllLookupFunction;
import com.dtstack.flinkx.connector.stream.lookup.StreamLruLookupFunction;

/**
 * @author chuixue
 * @create 2021-04-09 09:20
 * @description
 **/

public class StreamDynamicTableSource implements ScanTableSource, LookupTableSource, SupportsProjectionPushDown {

    private TableSchema physicalSchema;
    private String cache = "LRU";

    public StreamDynamicTableSource(TableSchema physicalSchema) {
        this.physicalSchema = physicalSchema;
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        if("LRU".equalsIgnoreCase(cache)){
            return AsyncTableFunctionProvider.of(new StreamLruLookupFunction());
        }
        return TableFunctionProvider.of(new StreamAllLookupFunction());
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return null;
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        // 如果sql没有source ,则此处不需要实现即可，目前sql只有kafka source(scan)
        return null;
    }

    @Override
    public DynamicTableSource copy() {
        return new StreamDynamicTableSource(this.physicalSchema);
    }

    @Override
    public String asSummaryString() {
        return "StreamDynamicTableSink:";
    }

    @Override
    public boolean supportsNestedProjection() {
        // 谓词下推开关
        return false;
    }

    @Override
    public void applyProjection(int[][] projectedFields) {
        // 下推字段，需要拼接到维表查询数据的语句中
        this.physicalSchema = TableSchemaUtils.projectSchema(physicalSchema, projectedFields);
    }
}
