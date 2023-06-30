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
import com.dtstack.chunjun.connector.mongodb.converter.MongodbSqlConverter;
import com.dtstack.chunjun.connector.mongodb.datasync.MongodbDataSyncConfig;
import com.dtstack.chunjun.connector.mongodb.source.MongodbInputFormatBuilder;
import com.dtstack.chunjun.connector.mongodb.table.lookup.MongoAllTableFunction;
import com.dtstack.chunjun.connector.mongodb.table.lookup.MongoLruTableFunction;
import com.dtstack.chunjun.connector.mongodb.table.options.MongoClientOptions;
import com.dtstack.chunjun.enums.CacheType;
import com.dtstack.chunjun.lookup.config.LookupConfig;
import com.dtstack.chunjun.source.DtInputFormatSourceFunction;
import com.dtstack.chunjun.source.options.SourceOptions;
import com.dtstack.chunjun.table.connector.source.ParallelAsyncLookupFunctionProvider;
import com.dtstack.chunjun.table.connector.source.ParallelLookupFunctionProvider;
import com.dtstack.chunjun.table.connector.source.ParallelSourceFunctionProvider;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import java.util.List;

public class MongodbDynamicTableSource implements ScanTableSource, LookupTableSource {

    private final MongoClientConfig mongoClientConfig;
    private final ResolvedSchema resolvedSchema;
    private final LookupConfig lookupConfig;
    private final ReadableConfig config;

    public MongodbDynamicTableSource(
            MongoClientConfig mongoClientConfig,
            LookupConfig lookupConfig,
            ResolvedSchema resolvedSchema,
            ReadableConfig config) {
        this.mongoClientConfig = mongoClientConfig;
        this.lookupConfig = lookupConfig;
        this.resolvedSchema = resolvedSchema;
        this.config = config;
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

    /**
     * ScanTableSource独有
     *
     * @param runtimeProviderContext
     * @return
     */
    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        final RowType rowType = (RowType) resolvedSchema.toPhysicalRowDataType().getLogicalType();
        TypeInformation<RowData> typeInformation = InternalTypeInfo.of(rowType);

        MongodbDataSyncConfig mongodbDataSyncConf = new MongodbDataSyncConfig();
        config.getOptional(MongoClientOptions.URI).ifPresent(mongodbDataSyncConf::setUrl);
        config.getOptional(MongoClientOptions.DATABASE).ifPresent(mongodbDataSyncConf::setDatabase);
        config.getOptional(MongoClientOptions.COLLECTION)
                .ifPresent(mongodbDataSyncConf::setCollectionName);
        config.getOptional(MongoClientOptions.USERNAME).ifPresent(mongodbDataSyncConf::setUsername);
        config.getOptional(MongoClientOptions.PASSWORD).ifPresent(mongodbDataSyncConf::setPassword);
        String filter = config.getOptional(MongoClientOptions.FILTER).orElse(null);
        String fetchsize = config.getOptional(MongoClientOptions.FETCH_SIZE).orElse("0");
        Integer parallelism = config.getOptional(SourceOptions.SCAN_PARALLELISM).orElse(1);

        MongodbInputFormatBuilder builder =
                MongodbInputFormatBuilder.newBuild(
                        mongoClientConfig, filter, Integer.parseInt(fetchsize));
        MongodbSqlConverter mongodbSqlConverter =
                new MongodbSqlConverter(
                        rowType, resolvedSchema.getColumnNames().toArray(new String[0]));

        builder.setRowConverter(mongodbSqlConverter);
        builder.setConfig(mongodbDataSyncConf);

        return ParallelSourceFunctionProvider.of(
                new DtInputFormatSourceFunction<>(builder.finish(), typeInformation),
                false,
                parallelism);
    }

    @Override
    public DynamicTableSource copy() {
        return new MongodbDynamicTableSource(
                this.mongoClientConfig, this.lookupConfig, this.resolvedSchema, this.config);
    }

    @Override
    public String asSummaryString() {
        return "MongoDB Source";
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }
}
