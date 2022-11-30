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

package com.dtstack.chunjun.connector.jdbc.sink;

import com.dtstack.chunjun.config.FieldConfig;
import com.dtstack.chunjun.connector.jdbc.config.JdbcConfig;
import com.dtstack.chunjun.connector.jdbc.dialect.JdbcDialect;
import com.dtstack.chunjun.enums.EWriteMode;
import com.dtstack.chunjun.sink.DtOutputFormatSinkFunction;

import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CollectionUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkState;

public class JdbcDynamicTableSink implements DynamicTableSink {

    protected final JdbcConfig jdbcConfig;
    protected final JdbcDialect jdbcDialect;
    protected final ResolvedSchema tableSchema;
    protected final String dialectName;
    protected final JdbcOutputFormatBuilder builder;

    public JdbcDynamicTableSink(
            JdbcConfig jdbcConfig,
            JdbcDialect jdbcDialect,
            ResolvedSchema tableSchema,
            JdbcOutputFormatBuilder builder) {
        this.jdbcConfig = jdbcConfig;
        this.jdbcDialect = jdbcDialect;
        this.tableSchema = tableSchema;
        this.dialectName = jdbcDialect.dialectName();
        this.builder = builder;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        validatePrimaryKey(requestedMode);
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.DELETE)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .addContainedKind(RowKind.UPDATE_BEFORE)
                .build();
    }

    private void validatePrimaryKey(ChangelogMode requestedMode) {
        checkState(
                ChangelogMode.insertOnly().equals(requestedMode)
                        || !CollectionUtil.isNullOrEmpty(jdbcConfig.getUniqueKey()),
                "please declare primary key for sink table when query contains update/delete record.");
    }

    @Override
    public SinkFunctionProvider getSinkRuntimeProvider(Context context) {
        // 通过该参数得到类型转换器，将数据库中的字段转成对应的类型
        final InternalTypeInfo<?> typeInformation =
                InternalTypeInfo.of(tableSchema.toPhysicalRowDataType().getLogicalType());

        JdbcOutputFormatBuilder builder = this.builder;

        List<Column> columns = tableSchema.getColumns();

        List<String> columnNameList = new ArrayList<>(columns.size());
        List<String> columnTypeList = new ArrayList<>(columns.size());
        List<FieldConfig> columnList = new ArrayList<>(columns.size());

        columns.forEach(
                column -> {
                    String name = column.getName();
                    String type = column.getDataType().getLogicalType().asSummaryString();
                    FieldConfig field = new FieldConfig();
                    columnNameList.add(name);
                    columnTypeList.add(type);
                    field.setName(name);
                    field.setType(type);
                    field.setIndex(columns.indexOf(column));
                    columnList.add(field);
                });

        jdbcConfig.setColumn(columnList);
        jdbcConfig.setMode(
                (CollectionUtil.isNullOrEmpty(jdbcConfig.getUniqueKey()))
                        ? EWriteMode.INSERT.name()
                        : EWriteMode.UPDATE.name());

        builder.setColumnNameList(columnNameList);
        builder.setColumnTypeList(columnTypeList);

        builder.setJdbcDialect(jdbcDialect);
        builder.setJdbcConf(jdbcConfig);
        builder.setRowConverter(jdbcDialect.getRowConverter(typeInformation.toRowType()));

        return SinkFunctionProvider.of(
                new DtOutputFormatSinkFunction<>(builder.finish()), jdbcConfig.getParallelism());
    }

    @Override
    public DynamicTableSink copy() {
        return new JdbcDynamicTableSink(jdbcConfig, jdbcDialect, tableSchema, builder);
    }

    @Override
    public String asSummaryString() {
        return "JDBC:" + dialectName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof JdbcDynamicTableSink)) {
            return false;
        }
        JdbcDynamicTableSink that = (JdbcDynamicTableSink) o;
        return Objects.equals(jdbcConfig, that.jdbcConfig)
                && Objects.equals(tableSchema, that.tableSchema)
                && Objects.equals(dialectName, that.dialectName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jdbcConfig, tableSchema, dialectName);
    }
}
