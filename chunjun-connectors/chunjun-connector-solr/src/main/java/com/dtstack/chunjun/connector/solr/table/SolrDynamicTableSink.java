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

package com.dtstack.chunjun.connector.solr.table;

import com.dtstack.chunjun.connector.solr.SolrConf;
import com.dtstack.chunjun.connector.solr.converter.SolrRowConverter;
import com.dtstack.chunjun.connector.solr.sink.SolrOutputFormatBuilder;
import com.dtstack.chunjun.sink.DtOutputFormatSinkFunction;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

/**
 * @author Ada Wong
 * @program chunjun
 * @create 2021/06/15
 */
public class SolrDynamicTableSink implements DynamicTableSink {

    private final SolrConf solrConf;
    private final TableSchema physicalSchema;

    public SolrDynamicTableSink(SolrConf solrConf, TableSchema physicalSchema) {
        this.solrConf = solrConf;
        this.physicalSchema = physicalSchema;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.DELETE)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .build();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        final RowType rowType = (RowType) physicalSchema.toRowDataType().getLogicalType();
        String[] fieldNames = physicalSchema.getFieldNames();

        SolrOutputFormatBuilder builder = new SolrOutputFormatBuilder(solrConf);
        builder.setRowConverter(new SolrRowConverter(rowType, fieldNames));

        return SinkFunctionProvider.of(
                new DtOutputFormatSinkFunction<>(builder.finish()), solrConf.getParallelism());
    }

    @Override
    public DynamicTableSink copy() {
        return new SolrDynamicTableSink(solrConf, physicalSchema);
    }

    @Override
    public String asSummaryString() {
        return "Solr Sink";
    }
}
