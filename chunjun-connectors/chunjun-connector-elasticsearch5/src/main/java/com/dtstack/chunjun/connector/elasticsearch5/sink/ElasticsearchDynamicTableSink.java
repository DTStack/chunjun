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

package com.dtstack.chunjun.connector.elasticsearch5.sink;

import com.dtstack.chunjun.connector.elasticsearch5.conf.ElasticsearchConf;
import com.dtstack.chunjun.connector.elasticsearch5.converter.ElasticsearchRowConverter;
import com.dtstack.chunjun.streaming.api.functions.sink.DtOutputFormatSinkFunction;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

/**
 * @description:
 * @program chunjun
 * @author: lany
 * @create: 2021/06/27 23:50
 */
public class ElasticsearchDynamicTableSink implements DynamicTableSink {
    private final TableSchema physicalSchema;
    private final ElasticsearchConf elasticsearchConf;

    public ElasticsearchDynamicTableSink(
            TableSchema physicalSchema, ElasticsearchConf elasticsearchConf) {
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

        ElasticsearchOutputFormatBuilder builder = new ElasticsearchOutputFormatBuilder();
        builder.setRowConverter(new ElasticsearchRowConverter(rowType));
        builder.setEsConf(elasticsearchConf);

        return SinkFunctionProvider.of(new DtOutputFormatSinkFunction<>(builder.finish()), 1);
    }

    @Override
    public DynamicTableSink copy() {
        return new ElasticsearchDynamicTableSink(physicalSchema, elasticsearchConf);
    }

    @Override
    public String asSummaryString() {
        return "Elasticsearch6 sink";
    }
}
