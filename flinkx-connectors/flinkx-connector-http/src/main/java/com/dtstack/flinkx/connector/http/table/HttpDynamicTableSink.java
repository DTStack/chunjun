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
package com.dtstack.flinkx.connector.http.table;

import com.dtstack.flinkx.conf.FieldConf;
import com.dtstack.flinkx.connector.http.common.HttpWriterConfig;
import com.dtstack.flinkx.connector.http.converter.HttpRowConverter;
import com.dtstack.flinkx.connector.http.outputformat.HttpOutputFormatBuilder;
import com.dtstack.flinkx.sink.DtOutputFormatSinkFunction;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import org.apache.commons.collections.CollectionUtils;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author: shifang
 * @description table sink
 * @date: 2021/9/15 下午5:48
 */
public class HttpDynamicTableSink implements DynamicTableSink {

    private final TableSchema schema;
    private final HttpWriterConfig restapiWriterConf;

    public HttpDynamicTableSink(TableSchema schema, HttpWriterConfig httpWriterConf) {
        this.schema = schema;
        this.restapiWriterConf = httpWriterConf;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode changelogMode) {
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.DELETE)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .build();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        final RowType rowType = (RowType) schema.toRowDataType().getLogicalType();
        List<FieldConf> fieldList =
                Arrays.stream(schema.getFieldNames())
                        .map(
                                e -> {
                                    FieldConf fieldConf = new FieldConf();
                                    fieldConf.setName(e);
                                    return fieldConf;
                                })
                        .collect(Collectors.toList());
        if (CollectionUtils.isNotEmpty(restapiWriterConf.getColumn())) {
            fieldList.addAll(restapiWriterConf.getColumn());
        }
        restapiWriterConf.setColumn(fieldList);
        HttpOutputFormatBuilder builder = new HttpOutputFormatBuilder();
        builder.setRowConverter(new HttpRowConverter(rowType));
        builder.setConfig(restapiWriterConf);

        return SinkFunctionProvider.of(new DtOutputFormatSinkFunction<>(builder.finish()), 1);
    }

    @Override
    public DynamicTableSink copy() {
        return new HttpDynamicTableSink(schema, restapiWriterConf);
    }

    @Override
    public String asSummaryString() {
        return "Restapi Sink";
    }
}
