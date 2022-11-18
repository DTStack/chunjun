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

import com.dtstack.chunjun.config.FieldConf;
import com.dtstack.chunjun.connector.starrocks.conf.StarRocksConf;
import com.dtstack.chunjun.connector.starrocks.converter.StarRocksRowConverter;
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

import java.util.ArrayList;
import java.util.List;

/** @author liuliu 2022/7/19 */
public class StarRocksDynamicTableSource
        implements ScanTableSource, LookupTableSource, SupportsProjectionPushDown {
    private final StarRocksConf starRocksConf;
    private final LookupConf lookupConf;
    private TableSchema tableSchema;

    public StarRocksDynamicTableSource(
            StarRocksConf starRocksConf, LookupConf lookupConf, TableSchema tableSchema) {
        this.starRocksConf = starRocksConf;
        this.lookupConf = lookupConf;
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
        final RowType rowType = (RowType) tableSchema.toRowDataType().getLogicalType();

        if (lookupConf.getCache().equalsIgnoreCase(CacheType.ALL.toString())) {
            return ParallelTableFunctionProvider.of(
                    new StarRocksAllTableFunction(
                            starRocksConf,
                            lookupConf,
                            keyIndexes,
                            new StarRocksRowConverter(rowType, null)),
                    lookupConf.getParallelism());
        }
        return ParallelAsyncTableFunctionProvider.of(
                new StarRocksLruTableFunction(
                        starRocksConf,
                        lookupConf,
                        keyIndexes,
                        new StarRocksRowConverter(rowType, null)),
                lookupConf.getParallelism());
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        final RowType rowType = (RowType) tableSchema.toRowDataType().getLogicalType();
        TypeInformation<RowData> typeInformation = InternalTypeInfo.of(rowType);
        StarRocksInputFormatBuilder builder =
                new StarRocksInputFormatBuilder(new StarRocksInputFormat());
        List<FieldConf> fieldConfList = new ArrayList<>(tableSchema.getFieldCount());
        String[] fieldNames = tableSchema.getFieldNames();
        for (int i = 0; i < tableSchema.getFieldCount(); i++) {
            FieldConf fieldConf = new FieldConf();
            fieldConf.setName(fieldNames[i]);
            fieldConf.setType(rowType.getTypeAt(i).asSummaryString());
            fieldConfList.add(fieldConf);
        }
        starRocksConf.setColumn(fieldConfList);
        builder.setStarRocksConf(starRocksConf);
        builder.setRowConverter(new StarRocksRowConverter(rowType, null));

        return ParallelSourceFunctionProvider.of(
                new DtInputFormatSourceFunction<>(builder.finish(), typeInformation),
                false,
                starRocksConf.getParallelism());
    }

    @Override
    public DynamicTableSource copy() {
        return new StarRocksDynamicTableSource(starRocksConf, lookupConf, tableSchema);
    }

    @Override
    public String asSummaryString() {
        return "StarRocks Source";
    }

    @Override
    public boolean supportsNestedProjection() {
        return false;
    }

    @Override
    public void applyProjection(int[][] projectedFields) {
        this.tableSchema = TableSchemaUtils.projectSchema(tableSchema, projectedFields);
    }
}
