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

package com.dtstack.chunjun.connector.elasticsearch7.table;

import com.dtstack.chunjun.connector.elasticsearch.ElasticsearchRowConverter;
import com.dtstack.chunjun.connector.elasticsearch7.ElasticsearchConf;
import com.dtstack.chunjun.connector.elasticsearch7.source.ElasticsearchInputFormatBuilder;
import com.dtstack.chunjun.connector.elasticsearch7.table.lookup.ElasticsearchAllTableFunction;
import com.dtstack.chunjun.connector.elasticsearch7.table.lookup.ElasticsearchLruTableFunction;
import com.dtstack.chunjun.enums.CacheType;
import com.dtstack.chunjun.lookup.config.LookupConf;
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
 * @description:
 * @program chunjun
 * @author: lany
 * @create: 2021/06/27 17:24
 */
public class ElasticsearchDynamicTableSource
        implements ScanTableSource, LookupTableSource, SupportsProjectionPushDown {

    protected final ElasticsearchConf elasticsearchConf;
    protected final LookupConf lookupConf;
    private TableSchema physicalSchema;

    public ElasticsearchDynamicTableSource(
            TableSchema physicalSchema,
            ElasticsearchConf elasticsearchConf,
            LookupConf lookupConf) {
        this.physicalSchema = physicalSchema;
        this.elasticsearchConf = elasticsearchConf;
        this.lookupConf = lookupConf;
    }

    @Override
    public DynamicTableSource copy() {
        return new ElasticsearchDynamicTableSource(physicalSchema, elasticsearchConf, lookupConf);
    }

    @Override
    public String asSummaryString() {
        return "Elasticsearch7 source.";
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        final RowType rowType = (RowType) physicalSchema.toRowDataType().getLogicalType();
        TypeInformation<RowData> typeInformation = InternalTypeInfo.of(rowType);

        ElasticsearchInputFormatBuilder builder = new ElasticsearchInputFormatBuilder();
        builder.setRowConverter(new ElasticsearchRowConverter(rowType));
        String[] fieldNames = physicalSchema.getFieldNames();
        elasticsearchConf.setFieldNames(fieldNames);
        builder.setEsConf(elasticsearchConf);

        return ParallelSourceFunctionProvider.of(
                new DtInputFormatSourceFunction<>(builder.finish(), typeInformation), false, 1);
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
        if (lookupConf.getCache().equalsIgnoreCase(CacheType.ALL.toString())) {
            return ParallelTableFunctionProvider.of(
                    new ElasticsearchAllTableFunction(
                            elasticsearchConf,
                            lookupConf,
                            physicalSchema.getFieldNames(),
                            keyNames,
                            new ElasticsearchRowConverter(rowType)),
                    lookupConf.getParallelism());
        }
        return ParallelAsyncTableFunctionProvider.of(
                new ElasticsearchLruTableFunction(
                        elasticsearchConf,
                        lookupConf,
                        physicalSchema.getFieldNames(),
                        keyNames,
                        new ElasticsearchRowConverter(rowType)),
                lookupConf.getParallelism());
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
