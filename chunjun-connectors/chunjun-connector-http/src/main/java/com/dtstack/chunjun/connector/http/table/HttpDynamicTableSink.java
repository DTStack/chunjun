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
package com.dtstack.chunjun.connector.http.table;

import com.dtstack.chunjun.config.FieldConfig;
import com.dtstack.chunjun.connector.http.common.HttpWriterConfig;
import com.dtstack.chunjun.connector.http.converter.HttpSqlConverter;
import com.dtstack.chunjun.connector.http.outputformat.HttpOutputFormatBuilder;
import com.dtstack.chunjun.sink.DtOutputFormatSinkFunction;

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
        List<FieldConfig> fieldList =
                Arrays.stream(schema.getFieldNames())
                        .map(
                                e -> {
                                    FieldConfig fieldConfig = new FieldConfig();
                                    fieldConfig.setName(e);
                                    return fieldConfig;
                                })
                        .collect(Collectors.toList());
        if (CollectionUtils.isNotEmpty(restapiWriterConf.getColumn())) {
            fieldList.addAll(restapiWriterConf.getColumn());
        }
        restapiWriterConf.setColumn(fieldList);
        HttpOutputFormatBuilder builder = new HttpOutputFormatBuilder();
        builder.setRowConverter(new HttpSqlConverter(rowType));
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
