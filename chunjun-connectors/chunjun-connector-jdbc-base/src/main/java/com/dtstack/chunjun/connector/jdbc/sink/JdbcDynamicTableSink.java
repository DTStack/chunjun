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

import com.dtstack.chunjun.conf.FieldConf;
import com.dtstack.chunjun.connector.jdbc.conf.JdbcConf;
import com.dtstack.chunjun.connector.jdbc.dialect.JdbcDialect;
import com.dtstack.chunjun.sink.DtOutputFormatSinkFunction;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CollectionUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * @author chuixue
 * @create 2021-04-12 14:44
 * @description
 */
public class JdbcDynamicTableSink implements DynamicTableSink {

    protected final JdbcConf jdbcConf;
    protected final JdbcDialect jdbcDialect;
    protected final TableSchema tableSchema;
    protected final String dialectName;
    protected final JdbcOutputFormatBuilder builder;

    public JdbcDynamicTableSink(
            JdbcConf jdbcConf,
            JdbcDialect jdbcDialect,
            TableSchema tableSchema,
            JdbcOutputFormatBuilder builder) {
        this.jdbcConf = jdbcConf;
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
                .build();
    }

    private void validatePrimaryKey(ChangelogMode requestedMode) {
        checkState(
                ChangelogMode.insertOnly().equals(requestedMode)
                        || !CollectionUtil.isNullOrEmpty(jdbcConf.getUniqueKey()),
                "please declare primary key for sink table when query contains update/delete record.");
    }

    @Override
    public SinkFunctionProvider getSinkRuntimeProvider(Context context) {
        // 通过该参数得到类型转换器，将数据库中的字段转成对应的类型
        final RowType rowType = (RowType) tableSchema.toRowDataType().getLogicalType();

        JdbcOutputFormatBuilder builder = this.builder;

        String[] fieldNames = tableSchema.getFieldNames();
        List<String> columnNameList = new ArrayList<>(fieldNames.length);
        List<String> columnTypeList = new ArrayList<>(fieldNames.length);
        List<FieldConf> columnList = new ArrayList<>(fieldNames.length);
        for (int i = 0; i < fieldNames.length; i++) {
            String name = fieldNames[i];
            String type = rowType.getTypeAt(i).asSummaryString();
            FieldConf field = new FieldConf();
            columnNameList.add(name);
            columnTypeList.add(type);
            field.setName(name);
            field.setType(type);
            field.setIndex(i);
            columnList.add(field);
        }
        jdbcConf.setColumn(columnList);

        builder.setColumnNameList(columnNameList);
        builder.setColumnTypeList(columnTypeList);

        builder.setJdbcDialect(jdbcDialect);
        builder.setJdbcConf(jdbcConf);
        builder.setRowConverter(jdbcDialect.getRowConverter(rowType));
        setKeyRowConverter(builder, rowType);

        return SinkFunctionProvider.of(
                new DtOutputFormatSinkFunction(builder.finish()), jdbcConf.getParallelism());
    }

    protected void setKeyRowConverter(JdbcOutputFormatBuilder builder, RowType rowType) {
        if (!CollectionUtil.isNullOrEmpty(jdbcConf.getUniqueKey())) {
            List<RowType.RowField> fields = rowType.getFields();
            List<RowType.RowField> keyRowFields = new ArrayList<>(jdbcConf.getUniqueKey().size());
            for (String name : jdbcConf.getUniqueKey()) {
                for (RowType.RowField field : fields) {
                    if (Objects.equals(name, field.getName())) {
                        keyRowFields.add(field);
                        break;
                    }
                }
            }
            RowType keyRowType = new RowType(keyRowFields);
            builder.setKeyRowType(keyRowType);
            builder.setKeyRowConverter(jdbcDialect.getRowConverter(keyRowType));
        }
    }

    @Override
    public DynamicTableSink copy() {
        return new JdbcDynamicTableSink(jdbcConf, jdbcDialect, tableSchema, builder);
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
        return Objects.equals(jdbcConf, that.jdbcConf)
                && Objects.equals(tableSchema, that.tableSchema)
                && Objects.equals(dialectName, that.dialectName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jdbcConf, tableSchema, dialectName);
    }
}
