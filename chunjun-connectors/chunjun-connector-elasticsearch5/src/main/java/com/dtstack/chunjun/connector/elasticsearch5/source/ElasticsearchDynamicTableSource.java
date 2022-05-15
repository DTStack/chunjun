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

package com.dtstack.chunjun.connector.elasticsearch5.source;

import com.dtstack.chunjun.connector.elasticsearch5.conf.ElasticsearchConf;
import com.dtstack.chunjun.connector.elasticsearch5.converter.ElasticsearchRowConverter;
import com.dtstack.chunjun.lookup.conf.LookupConf;
import com.dtstack.chunjun.streaming.api.functions.source.DtInputFormatSourceFunction;
import com.dtstack.chunjun.table.connector.source.ParallelSourceFunctionProvider;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.utils.TableSchemaUtils;

/**
 * @description:
 * @program chunjun
 * @author: lany
 * @create: 2021/06/28 00:04
 */
public class ElasticsearchDynamicTableSource
        implements ScanTableSource, SupportsProjectionPushDown {

    private TableSchema physicalSchema;
    protected final ElasticsearchConf elasticsearchConf;
    protected final LookupConf lookupConf;

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

        ElasticsearchInputFormatBuilder builder = new ElasticsearchInputFormatBuilder();
        builder.setRowConverter(new ElasticsearchRowConverter(rowType));
        builder.setEsConf(elasticsearchConf);

        return ParallelSourceFunctionProvider.of(
                new DtInputFormatSourceFunction<>(builder.finish(), typeInformation), false, 1);
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
