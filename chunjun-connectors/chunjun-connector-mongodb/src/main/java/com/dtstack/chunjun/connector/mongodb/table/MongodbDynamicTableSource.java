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

import com.dtstack.chunjun.connector.mongodb.conf.MongoClientConf;
import com.dtstack.chunjun.connector.mongodb.table.lookup.MongoAllTableFunction;
import com.dtstack.chunjun.connector.mongodb.table.lookup.MongoLruTableFunction;
import com.dtstack.chunjun.enums.CacheType;
import com.dtstack.chunjun.lookup.conf.LookupConf;
import com.dtstack.chunjun.table.connector.source.ParallelAsyncTableFunctionProvider;
import com.dtstack.chunjun.table.connector.source.ParallelTableFunctionProvider;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

/**
 * @author Ada Wong
 * @program flinkx
 * @create 2021/06/21
 */
public class MongodbDynamicTableSource implements ScanTableSource, LookupTableSource {

    private final MongoClientConf mongoClientConf;
    private final TableSchema physicalSchema;
    private final LookupConf lookupConf;

    public MongodbDynamicTableSource(
            MongoClientConf mongoClientConf, LookupConf lookupConf, TableSchema physicalSchema) {
        this.mongoClientConf = mongoClientConf;
        this.lookupConf = lookupConf;
        this.physicalSchema = physicalSchema;
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        // 获取JOIN Key 名字
        String[] keyNames = new String[context.getKeys().length];
        for (int i = 0; i < keyNames.length; i++) {
            int[] innerKeyArr = context.getKeys()[i];
            Preconditions.checkArgument(
                    innerKeyArr.length == 1, "MongoDB only support non-nested look up keys");
            keyNames[i] = physicalSchema.getFieldNames()[innerKeyArr[0]];
        }

        // 通过该参数得到类型转换器，将数据库中的字段转成对应的类型
        final RowType rowType = (RowType) physicalSchema.toRowDataType().getLogicalType();
        if (lookupConf.getCache().equalsIgnoreCase(CacheType.ALL.toString())) {
            return ParallelTableFunctionProvider.of(
                    new MongoAllTableFunction(
                            mongoClientConf,
                            lookupConf,
                            rowType,
                            keyNames,
                            physicalSchema.getFieldNames()),
                    lookupConf.getParallelism());
        } else {
            return ParallelAsyncTableFunctionProvider.of(
                    new MongoLruTableFunction(
                            mongoClientConf,
                            lookupConf,
                            rowType,
                            keyNames,
                            physicalSchema.getFieldNames()),
                    lookupConf.getParallelism());
        }
    }

    @Override
    public DynamicTableSource copy() {
        return new MongodbDynamicTableSource(
                this.mongoClientConf, this.lookupConf, this.physicalSchema);
    }

    @Override
    public String asSummaryString() {
        return "MongoDB Source";
    }

    /**
     * ScanTableSource独有
     *
     * @return
     */
    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    /**
     * ScanTableSource独有
     *
     * @param runtimeProviderContext
     * @return
     */
    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        return null;
    }
}
