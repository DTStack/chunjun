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

package com.dtstack.flinkx.connector.inceptor.sink;

import com.dtstack.flinkx.conf.FieldConf;
import com.dtstack.flinkx.connector.inceptor.conf.InceptorConf;
import com.dtstack.flinkx.connector.jdbc.dialect.JdbcDialect;
import com.dtstack.flinkx.connector.jdbc.sink.JdbcDynamicTableSink;
import com.dtstack.flinkx.connector.jdbc.sink.JdbcOutputFormatBuilder;
import com.dtstack.flinkx.sink.DtOutputFormatSinkFunction;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * @author chuixue
 * @create 2021-04-12 14:44
 * @description
 */
public class InceptorDynamicTableSink extends JdbcDynamicTableSink {

    private final InceptorConf inceptorConf;

    public InceptorDynamicTableSink(
            JdbcDialect jdbcDialect,
            TableSchema tableSchema,
            JdbcOutputFormatBuilder builder,
            InceptorConf inceptorConf) {
        super(inceptorConf, jdbcDialect, tableSchema, builder);
        this.inceptorConf = inceptorConf;
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
    public SinkFunctionProvider getSinkRuntimeProvider(Context context) {
        // 通过该参数得到类型转换器，将数据库中的字段转成对应的类型
        final RowType rowType = (RowType) tableSchema.toRowDataType().getLogicalType();

        InceptorOutputFormatBuilder builder = (InceptorOutputFormatBuilder) this.builder;

        String[] fieldNames = tableSchema.getFieldNames();
        List<FieldConf> columnList = new ArrayList<>(fieldNames.length);
        for (int i = 0; i < fieldNames.length; i++) {
            FieldConf field = new FieldConf();
            field.setName(fieldNames[i]);
            field.setType(rowType.getTypeAt(i).asSummaryString());
            field.setIndex(i);
            columnList.add(field);
        }
        inceptorConf.setColumn(columnList);

        builder.setJdbcDialect(jdbcDialect);
        builder.setInceptorConf(inceptorConf);
        builder.setRowConverter(jdbcDialect.getRowConverter(rowType));

        return SinkFunctionProvider.of(
                new DtOutputFormatSinkFunction(builder.finish()), inceptorConf.getParallelism());
    }

    @Override
    public DynamicTableSink copy() {
        return new InceptorDynamicTableSink(jdbcDialect, tableSchema, builder, inceptorConf);
    }

    @Override
    public String asSummaryString() {
        return "InceptorDynamicTableSink";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        InceptorDynamicTableSink that = (InceptorDynamicTableSink) o;
        return Objects.equals(inceptorConf, that.inceptorConf);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), inceptorConf);
    }
}
