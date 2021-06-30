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

package com.dtstack.flinkx.connector.es.source;

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

import com.dtstack.flinkx.connector.es.conf.EsConf;
import com.dtstack.flinkx.connector.es.converter.EsRowConverter;
import com.dtstack.flinkx.connector.es.lookup.EsAllTableFunction;
import com.dtstack.flinkx.connector.es.lookup.EsLruTableFunction;
import com.dtstack.flinkx.enums.CacheType;
import com.dtstack.flinkx.lookup.conf.LookupConf;
import com.dtstack.flinkx.streaming.api.functions.source.DtInputFormatSourceFunction;
import com.dtstack.flinkx.table.connector.source.ParallelAsyncTableFunctionProvider;
import com.dtstack.flinkx.table.connector.source.ParallelSourceFunctionProvider;
import com.dtstack.flinkx.table.connector.source.ParallelTableFunctionProvider;

/**
 * @description:
 * @program: flinkx-all
 * @author: lany
 * @create: 2021/06/22 11:08
 */
public class EsDynamicTableSource implements ScanTableSource, LookupTableSource, SupportsProjectionPushDown {

    private TableSchema physicalSchema;
    protected final EsConf elasticsearchConf;
    protected final LookupConf lookupConf;

    public EsDynamicTableSource(TableSchema physicalSchema, EsConf elasticsearchConf, LookupConf lookupConf) {
        this.physicalSchema = physicalSchema;
        this.elasticsearchConf = elasticsearchConf;
        this.lookupConf = lookupConf;

    }

    @Override
    public DynamicTableSource copy() {
        return new EsDynamicTableSource(physicalSchema, elasticsearchConf, lookupConf);
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
        final RowType rowType = (RowType) physicalSchema.toRowDataType().getLogicalType();
        TypeInformation<RowData> typeInformation = InternalTypeInfo.of(rowType);

        EsInputFormatBuilder builder = new EsInputFormatBuilder();
        builder.setRowConverter(new EsRowConverter(rowType));
        builder.setEsConf(elasticsearchConf);

        return ParallelSourceFunctionProvider.of(
                new DtInputFormatSourceFunction<>(builder.finish(), typeInformation),
                false,
                1);
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        String[] keyNames = new String[context.getKeys().length];
        for (int i = 0; i < keyNames.length; i++) {
            int[] innerKeyArr = context.getKeys()[i];
            Preconditions.checkArgument(
                    innerKeyArr.length == 1, "elasticsearch only support non-nested look up keys");
            keyNames[i] = physicalSchema.getFieldNames()[innerKeyArr[0]];
        }

        final RowType rowType = (RowType) physicalSchema.toRowDataType().getLogicalType();
        if (lookupConf.getCache().equalsIgnoreCase(CacheType.LRU.toString())) {
            return ParallelAsyncTableFunctionProvider.of(
                    new EsLruTableFunction(
                            elasticsearchConf,
                            lookupConf,
                            physicalSchema.getFieldNames(),
                            keyNames,
                            new EsRowConverter(rowType)
                    ),
                    lookupConf.getParallelism()
            );
        }

        return ParallelTableFunctionProvider.of(
                new EsAllTableFunction(
                        elasticsearchConf,
                        lookupConf,
                        physicalSchema.getFieldNames(),
                        keyNames,
                        new EsRowConverter(rowType)
                ),
                lookupConf.getParallelism()
        );
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
