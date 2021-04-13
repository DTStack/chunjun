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
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.util.Preconditions;

import com.dtstack.flinkx.connector.stream.conf.StreamLookupConf;
import com.dtstack.flinkx.connector.stream.lookup.StreamAllLookupFunctionAll;
import com.dtstack.flinkx.connector.stream.lookup.StreamLruLookupFunction;
import com.dtstack.flinkx.enums.CacheType;

/**
 * @author chuixue
 * @create 2021-04-09 09:20
 * @description
 **/

public class StreamDynamicTableSource implements ScanTableSource, LookupTableSource, SupportsProjectionPushDown {

    private TableSchema physicalSchema;
    private StreamLookupConf lookupOptions;

    public StreamDynamicTableSource(StreamLookupConf lookupOptions, TableSchema physicalSchema) {
        this.lookupOptions = lookupOptions;
        this.physicalSchema = physicalSchema;
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {

        String[] keyNames = new String[context.getKeys().length];
        for (int i = 0; i < keyNames.length; i++) {
            int[] innerKeyArr = context.getKeys()[i];
            Preconditions.checkArgument(
                    innerKeyArr.length == 1, "JDBC only support non-nested look up keys");
            keyNames[i] = physicalSchema.getFieldNames()[innerKeyArr[0]];
        }
        final RowType rowType = (RowType) physicalSchema.toRowDataType().getLogicalType();

        if (lookupOptions.getCache().equalsIgnoreCase(CacheType.LRU.toString())) {
            return AsyncTableFunctionProvider.of(new StreamLruLookupFunction(
                    lookupOptions,
                    physicalSchema.getFieldNames(),
                    physicalSchema.getFieldDataTypes(),
                    keyNames
            ));
        }
        return TableFunctionProvider.of(new StreamAllLookupFunctionAll(
                lookupOptions,
                physicalSchema.getFieldNames(),
                keyNames));
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        // 如果sql没有source ,则此处不需要实现即可，目前sql只有kafka source(scan)
        return null;
    }

    @Override
    public DynamicTableSource copy() {
        return new StreamDynamicTableSource(this.lookupOptions, this.physicalSchema);
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
