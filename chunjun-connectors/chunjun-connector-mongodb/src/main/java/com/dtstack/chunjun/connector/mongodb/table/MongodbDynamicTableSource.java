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

package com.dtstack.chunjun.connector.mongodb.table;

import com.dtstack.chunjun.connector.mongodb.config.MongoClientConfig;
import com.dtstack.chunjun.connector.mongodb.table.lookup.MongoAllTableFunction;
import com.dtstack.chunjun.connector.mongodb.table.lookup.MongoLruTableFunction;
import com.dtstack.chunjun.enums.CacheType;
import com.dtstack.chunjun.lookup.config.LookupConfig;
import com.dtstack.chunjun.table.connector.source.ParallelAsyncLookupFunctionProvider;
import com.dtstack.chunjun.table.connector.source.ParallelLookupFunctionProvider;

import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import java.util.List;

public class MongodbDynamicTableSource implements ScanTableSource, LookupTableSource {

    private final MongoClientConfig mongoClientConfig;
    private final ResolvedSchema resolvedSchema;
    private final LookupConfig lookupConfig;

    public MongodbDynamicTableSource(
            MongoClientConfig mongoClientConfig,
            LookupConfig lookupConfig,
            ResolvedSchema resolvedSchema) {
        this.mongoClientConfig = mongoClientConfig;
        this.lookupConfig = lookupConfig;
        this.resolvedSchema = resolvedSchema;
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        // 获取JOIN Key 名字
        String[] keyNames = new String[context.getKeys().length];
        List<String> columnNames = resolvedSchema.getColumnNames();
        for (int i = 0; i < keyNames.length; i++) {
            int[] innerKeyArr = context.getKeys()[i];
            Preconditions.checkArgument(
                    innerKeyArr.length == 1, "MongoDB only support non-nested look up keys");
            keyNames[i] = columnNames.get(innerKeyArr[0]);
        }

        // 通过该参数得到类型转换器，将数据库中的字段转成对应的类型
        final RowType rowType = (RowType) resolvedSchema.toPhysicalRowDataType().getLogicalType();
        if (lookupConfig.getCache().equalsIgnoreCase(CacheType.ALL.toString())) {
            return ParallelLookupFunctionProvider.of(
                    new MongoAllTableFunction(
                            mongoClientConfig,
                            lookupConfig,
                            rowType,
                            keyNames,
                            columnNames.toArray(new String[0])),
                    lookupConfig.getParallelism());
        } else {
            return ParallelAsyncLookupFunctionProvider.of(
                    new MongoLruTableFunction(
                            mongoClientConfig,
                            lookupConfig,
                            rowType,
                            keyNames,
                            columnNames.toArray(new String[0])),
                    lookupConfig.getParallelism());
        }
    }

    @Override
    public DynamicTableSource copy() {
        return new MongodbDynamicTableSource(
                this.mongoClientConfig, this.lookupConfig, this.resolvedSchema);
    }

    @Override
    public String asSummaryString() {
        return "MongoDB Source";
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        return null;
    }
}
