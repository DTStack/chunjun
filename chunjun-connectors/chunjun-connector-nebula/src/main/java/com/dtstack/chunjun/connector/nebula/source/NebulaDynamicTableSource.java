package com.dtstack.chunjun.connector.nebula.source;
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

import com.dtstack.chunjun.connector.nebula.conf.NebulaConf;
import com.dtstack.chunjun.connector.nebula.converter.NebulaRowConverter;
import com.dtstack.chunjun.connector.nebula.lookup.NebulaAllTableFunction;
import com.dtstack.chunjun.connector.nebula.lookup.NebulaLruTableFunction;
import com.dtstack.chunjun.enums.CacheType;
import com.dtstack.chunjun.lookup.conf.LookupConf;
import com.dtstack.chunjun.source.DtInputFormatSourceFunction;
import com.dtstack.chunjun.table.connector.source.ParallelAsyncTableFunctionProvider;
import com.dtstack.chunjun.table.connector.source.ParallelSourceFunctionProvider;
import com.dtstack.chunjun.table.connector.source.ParallelTableFunctionProvider;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.util.Preconditions;

/**
 * @author: gaoasi
 * @email: aschaser@163.com
 * @date: 2022/11/7 3:59 下午
 */
public class NebulaDynamicTableSource
        implements ScanTableSource, LookupTableSource, SupportsProjectionPushDown {

    protected NebulaConf nebulaConf;

    protected TableSchema physicalSchema;

    protected final NebulaInputFormatBuilder builder;

    protected LookupConf lookupConf;

    public NebulaDynamicTableSource(
            NebulaConf nebulaConf,
            LookupConf lookupConf,
            TableSchema physicalSchema,
            NebulaInputFormatBuilder builder) {
        this.nebulaConf = nebulaConf;
        this.builder = builder;
        this.physicalSchema = physicalSchema;
        this.lookupConf = lookupConf;
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        String[] keyNames = new String[context.getKeys().length];
        for (int i = 0; i < keyNames.length; i++) {
            int[] innerKeyArr = context.getKeys()[i];
            Preconditions.checkArgument(
                    innerKeyArr.length == 1, "nebula only support non-nested look up keys");
            keyNames[i] = physicalSchema.getFieldNames()[innerKeyArr[0]];
        }
        final RowType rowType = (RowType) physicalSchema.toRowDataType().getLogicalType();

        if (CacheType.LRU.toString().equalsIgnoreCase(lookupConf.getCache())) {
            return ParallelAsyncTableFunctionProvider.of(
                    new NebulaLruTableFunction(
                            nebulaConf,
                            lookupConf,
                            physicalSchema.getFieldNames(),
                            keyNames,
                            new NebulaRowConverter(rowType)),
                    lookupConf.getParallelism());
        }
        return ParallelTableFunctionProvider.of(
                new NebulaAllTableFunction(
                        nebulaConf,
                        physicalSchema.getFieldNames(),
                        keyNames,
                        lookupConf,
                        new NebulaRowConverter(rowType)),
                lookupConf.getParallelism());
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        final RowType rowType = (RowType) physicalSchema.toRowDataType().getLogicalType();
        TypeInformation<RowData> typeInformation = InternalTypeInfo.of(rowType);
        builder.setNebulaConf(nebulaConf);
        builder.setRowConverter(new NebulaRowConverter(rowType));
        return ParallelSourceFunctionProvider.of(
                new DtInputFormatSourceFunction<>(builder.finish(), typeInformation),
                false,
                nebulaConf.getReadTasks());
    }

    @Override
    public DynamicTableSource copy() {
        return new NebulaDynamicTableSource(nebulaConf, lookupConf, physicalSchema, builder);
    }

    @Override
    public String asSummaryString() {
        return "nebula";
    }

    @Override
    public boolean supportsNestedProjection() {
        return false;
    }

    @Override
    public void applyProjection(int[][] projectedFields) {
        this.physicalSchema = TableSchemaUtils.projectSchema(physicalSchema, projectedFields);
    }
}
