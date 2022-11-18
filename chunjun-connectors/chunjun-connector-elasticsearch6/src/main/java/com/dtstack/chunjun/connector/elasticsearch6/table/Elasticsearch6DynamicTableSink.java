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
import com.dtstack.chunjun.connector.elasticsearch6.sink.Elasticsearch6OutputFormatBuilder;
import com.dtstack.chunjun.sink.DtOutputFormatSinkFunction;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

public class Elasticsearch6DynamicTableSink implements DynamicTableSink {

    private final TableSchema physicalSchema;
    private final Elasticsearch6Config elasticsearchConf;

    public Elasticsearch6DynamicTableSink(
            TableSchema physicalSchema, Elasticsearch6Config elasticsearchConf) {
        this.physicalSchema = physicalSchema;
        this.elasticsearchConf = elasticsearchConf;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        ChangelogMode.Builder builder = ChangelogMode.newBuilder();
        for (RowKind kind : requestedMode.getContainedKinds()) {
            if (kind != RowKind.UPDATE_BEFORE) {
                builder.addContainedKind(kind);
            }
        }
        return builder.build();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        final RowType rowType = (RowType) physicalSchema.toRowDataType().getLogicalType();

        Elasticsearch6OutputFormatBuilder builder = new Elasticsearch6OutputFormatBuilder();
        builder.setRowConverter(new ElasticsearchRowConverter(rowType));
        builder.setEsConf(elasticsearchConf);

        return SinkFunctionProvider.of(
                new DtOutputFormatSinkFunction<>(builder.finish()),
                elasticsearchConf.getParallelism());
    }

    @Override
    public DynamicTableSink copy() {
        return new Elasticsearch6DynamicTableSink(physicalSchema, elasticsearchConf);
    }

    @Override
    public String asSummaryString() {
        return "Elasticsearch6 sink";
    }
}
