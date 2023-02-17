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

package com.dtstack.chunjun.connector.elasticsearch6.table;

import com.dtstack.chunjun.connector.elasticsearch.ElasticsearchRowConverter;
import com.dtstack.chunjun.connector.elasticsearch6.Elasticsearch6Config;
import com.dtstack.chunjun.connector.elasticsearch6.source.Elasticsearch6InputFormatBuilder;
import com.dtstack.chunjun.connector.elasticsearch6.table.lookup.Elasticsearch6AllTableFunction;
import com.dtstack.chunjun.connector.elasticsearch6.table.lookup.Elasticsearch6LruTableFunction;
import com.dtstack.chunjun.enums.CacheType;
import com.dtstack.chunjun.lookup.config.LookupConfig;
import com.dtstack.chunjun.source.DtInputFormatSourceFunction;
import com.dtstack.chunjun.table.connector.source.ParallelAsyncLookupFunctionProvider;
import com.dtstack.chunjun.table.connector.source.ParallelLookupFunctionProvider;
import com.dtstack.chunjun.table.connector.source.ParallelSourceFunctionProvider;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.Preconditions;

public class Elasticsearch6DynamicTableSource
        implements ScanTableSource, LookupTableSource, SupportsProjectionPushDown {

    private final ResolvedSchema physicalSchema;
    protected final Elasticsearch6Config elasticsearchConfig;
    protected final LookupConfig lookupConfig;

    public Elasticsearch6DynamicTableSource(
            ResolvedSchema physicalSchema,
            Elasticsearch6Config elasticsearchConfig,
            LookupConfig lookupConfig) {
        this.physicalSchema = physicalSchema;
        this.elasticsearchConfig = elasticsearchConfig;
        this.lookupConfig = lookupConfig;
    }

    @Override
    public DynamicTableSource copy() {
        return new Elasticsearch6DynamicTableSource(
                physicalSchema, elasticsearchConfig, lookupConfig);
    }

    @Override
    public String asSummaryString() {
        return "Elasticsearch6 source.";
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        LogicalType logicalType = physicalSchema.toPhysicalRowDataType().getLogicalType();
        TypeInformation<RowData> typeInformation = InternalTypeInfo.of(logicalType);

        Elasticsearch6InputFormatBuilder builder = new Elasticsearch6InputFormatBuilder();
        builder.setRowConverter(
                new ElasticsearchRowConverter(InternalTypeInfo.of(logicalType).toRowType()));
        String[] fieldNames = physicalSchema.getColumnNames().toArray(new String[0]);
        elasticsearchConfig.setFieldNames(fieldNames);
        builder.setEsConf(elasticsearchConfig);

        return ParallelSourceFunctionProvider.of(
                new DtInputFormatSourceFunction<>(builder.finish(), typeInformation),
                false,
                elasticsearchConfig.getParallelism());
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        String[] keyNames = new String[context.getKeys().length];
        for (int i = 0; i < keyNames.length; i++) {
            int[] innerKeyArr = context.getKeys()[i];
            Preconditions.checkArgument(
                    innerKeyArr.length == 1, "elasticsearch only support non-nested look up keys");
            keyNames[i] = physicalSchema.getColumnNames().toArray(new String[0])[innerKeyArr[0]];
        }

        LogicalType logicalType = physicalSchema.toPhysicalRowDataType().getLogicalType();
        if (lookupConfig.getCache().equalsIgnoreCase(CacheType.ALL.toString())) {
            return ParallelLookupFunctionProvider.of(
                    new Elasticsearch6AllTableFunction(
                            elasticsearchConfig,
                            lookupConfig,
                            physicalSchema.getColumnNames().toArray(new String[0]),
                            keyNames,
                            new ElasticsearchRowConverter(
                                    InternalTypeInfo.of(logicalType).toRowType())),
                    lookupConfig.getParallelism());
        }
        return ParallelAsyncLookupFunctionProvider.of(
                new Elasticsearch6LruTableFunction(
                        elasticsearchConfig,
                        lookupConfig,
                        physicalSchema.getColumnNames().toArray(new String[0]),
                        keyNames,
                        new ElasticsearchRowConverter(
                                InternalTypeInfo.of(logicalType).toRowType())),
                lookupConfig.getParallelism());
    }

    @Override
    public boolean supportsNestedProjection() {
        return false;
    }
}
