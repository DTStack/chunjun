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

package com.dtstack.chunjun.connector.starrocks.source;

import com.dtstack.chunjun.config.FieldConfig;
import com.dtstack.chunjun.config.TypeConfig;
import com.dtstack.chunjun.connector.starrocks.config.StarRocksConfig;
import com.dtstack.chunjun.connector.starrocks.converter.StarRocksSqlConverter;
import com.dtstack.chunjun.enums.CacheType;
import com.dtstack.chunjun.lookup.config.LookupConfig;
import com.dtstack.chunjun.source.DtInputFormatSourceFunction;
import com.dtstack.chunjun.table.connector.source.ParallelAsyncLookupFunctionProvider;
import com.dtstack.chunjun.table.connector.source.ParallelLookupFunctionProvider;
import com.dtstack.chunjun.table.connector.source.ParallelSourceFunctionProvider;

import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.List;

public class StarRocksDynamicTableSource
        implements ScanTableSource, LookupTableSource, SupportsProjectionPushDown {
    private final StarRocksConfig starRocksConfig;
    private final LookupConfig lookupConfig;
    private final ResolvedSchema tableSchema;

    public StarRocksDynamicTableSource(
            StarRocksConfig starRocksConfig,
            LookupConfig lookupConfig,
            ResolvedSchema tableSchema) {
        this.starRocksConfig = starRocksConfig;
        this.lookupConfig = lookupConfig;
        this.tableSchema = tableSchema;
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        int[] keyIndexes = new int[context.getKeys().length];
        for (int i = 0; i < keyIndexes.length; i++) {
            int[] innerKeyArr = context.getKeys()[i];
            Preconditions.checkArgument(
                    innerKeyArr.length == 1, "StarRocks only support non-nested look up keys");
            keyIndexes[i] = innerKeyArr[0];
        }

        if (lookupConfig.getCache().equalsIgnoreCase(CacheType.ALL.toString())) {
            return ParallelLookupFunctionProvider.of(
                    new StarRocksAllTableFunction(
                            starRocksConfig,
                            lookupConfig,
                            keyIndexes,
                            new StarRocksSqlConverter(
                                    InternalTypeInfo.of(
                                                    tableSchema
                                                            .toPhysicalRowDataType()
                                                            .getLogicalType())
                                            .toRowType(),
                                    tableSchema.getColumnNames())),
                    lookupConfig.getParallelism());
        }
        return ParallelAsyncLookupFunctionProvider.of(
                new StarRocksLruTableFunction(
                        starRocksConfig,
                        lookupConfig,
                        keyIndexes,
                        new StarRocksSqlConverter(
                                InternalTypeInfo.of(
                                                tableSchema
                                                        .toPhysicalRowDataType()
                                                        .getLogicalType())
                                        .toRowType(),
                                tableSchema.getColumnNames())),
                lookupConfig.getParallelism());
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        StarRocksInputFormatBuilder builder =
                new StarRocksInputFormatBuilder(new StarRocksInputFormat());
        List<Column> columns = tableSchema.getColumns();
        List<FieldConfig> fieldConfList = new ArrayList<>(columns.size());
        for (Column column : columns) {
            FieldConfig fieldConfig = new FieldConfig();
            fieldConfig.setName(column.getName());
            fieldConfig.setType(
                    TypeConfig.fromString(column.getDataType().getLogicalType().asSummaryString()));
            fieldConfList.add(fieldConfig);
        }
        starRocksConfig.setColumn(fieldConfList);
        builder.setStarRocksConf(starRocksConfig);
        builder.setRowConverter(
                new StarRocksSqlConverter(
                        InternalTypeInfo.of(tableSchema.toPhysicalRowDataType().getLogicalType())
                                .toRowType(),
                        tableSchema.getColumnNames()));

        return ParallelSourceFunctionProvider.of(
                new DtInputFormatSourceFunction<>(
                        builder.finish(),
                        InternalTypeInfo.of(tableSchema.toPhysicalRowDataType().getLogicalType())),
                false,
                starRocksConfig.getParallelism());
    }

    @Override
    public DynamicTableSource copy() {
        return new StarRocksDynamicTableSource(starRocksConfig, lookupConfig, tableSchema);
    }

    @Override
    public String asSummaryString() {
        return "StarRocks Source";
    }

    @Override
    public boolean supportsNestedProjection() {
        return false;
    }
}
