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

package com.dtstack.chunjun.connector.nebula.source;

import com.dtstack.chunjun.connector.nebula.config.NebulaConfig;
import com.dtstack.chunjun.connector.nebula.converter.NebulaSqlConverter;
import com.dtstack.chunjun.connector.nebula.lookup.NebulaAllTableFunction;
import com.dtstack.chunjun.connector.nebula.lookup.NebulaLruTableFunction;
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
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

public class NebulaDynamicTableSource
        implements ScanTableSource, LookupTableSource, SupportsProjectionPushDown {

    protected NebulaConfig nebulaConfig;

    protected ResolvedSchema resolvedSchema;

    protected final NebulaInputFormatBuilder builder;

    protected LookupConfig lookupConf;

    public NebulaDynamicTableSource(
            NebulaConfig nebulaConfig,
            LookupConfig lookupConf,
            ResolvedSchema resolvedSchema,
            NebulaInputFormatBuilder builder) {
        this.nebulaConfig = nebulaConfig;
        this.builder = builder;
        this.resolvedSchema = resolvedSchema;
        this.lookupConf = lookupConf;
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        String[] keyNames = new String[context.getKeys().length];
        for (int i = 0; i < keyNames.length; i++) {
            int[] innerKeyArr = context.getKeys()[i];
            Preconditions.checkArgument(
                    innerKeyArr.length == 1, "nebula only support non-nested look up keys");
            keyNames[i] = resolvedSchema.getColumnNames().get(innerKeyArr[0]);
        }
        final RowType rowType =
                InternalTypeInfo.of(resolvedSchema.toPhysicalRowDataType().getLogicalType())
                        .toRowType();

        if (CacheType.LRU.toString().equalsIgnoreCase(lookupConf.getCache())) {
            return ParallelAsyncLookupFunctionProvider.of(
                    new NebulaLruTableFunction(
                            nebulaConfig,
                            lookupConf,
                            resolvedSchema.getColumnNames().toArray(new String[0]),
                            keyNames,
                            new NebulaSqlConverter(rowType)),
                    lookupConf.getParallelism());
        }
        return ParallelLookupFunctionProvider.of(
                new NebulaAllTableFunction(
                        nebulaConfig,
                        resolvedSchema.getColumnNames().toArray(new String[0]),
                        keyNames,
                        lookupConf,
                        new NebulaSqlConverter(rowType)),
                lookupConf.getParallelism());
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        final RowType rowType =
                InternalTypeInfo.of(resolvedSchema.toPhysicalRowDataType().getLogicalType())
                        .toRowType();
        TypeInformation<RowData> typeInformation = InternalTypeInfo.of(rowType);
        builder.setNebulaConf(nebulaConfig);
        builder.setRowConverter(new NebulaSqlConverter(rowType));
        return ParallelSourceFunctionProvider.of(
                new DtInputFormatSourceFunction<>(builder.finish(), typeInformation),
                false,
                nebulaConfig.getReadTasks());
    }

    @Override
    public DynamicTableSource copy() {
        return new NebulaDynamicTableSource(nebulaConfig, lookupConf, resolvedSchema, builder);
    }

    @Override
    public String asSummaryString() {
        return "nebula";
    }

    @Override
    public boolean supportsNestedProjection() {
        return false;
    }
}
