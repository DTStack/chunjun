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

package com.dtstack.flinkx.connector.jdbc.table;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcDmlOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcOptions;
import org.apache.flink.connector.jdbc.table.JdbcDynamicOutputFormatBuilder;
import org.apache.flink.connector.jdbc.table.JdbcDynamicTableSink;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.OutputFormatProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * @program: flinkx
 * @author: wuren
 * @create: 2021/03/26
 **/
public abstract class DtJdbcDynamicTableSink extends JdbcDynamicTableSink {

    protected final JdbcOptions jdbcOptions;
    protected final JdbcExecutionOptions executionOptions;
    protected final JdbcDmlOptions dmlOptions;
    protected final TableSchema tableSchema;
    protected final String dialectName;

    public DtJdbcDynamicTableSink(JdbcOptions jdbcOptions, JdbcExecutionOptions executionOptions, JdbcDmlOptions dmlOptions, TableSchema tableSchema) {
        super(jdbcOptions, executionOptions, dmlOptions, tableSchema);
        this.jdbcOptions = jdbcOptions;
        this.executionOptions = executionOptions;
        this.dmlOptions = dmlOptions;
        this.tableSchema = tableSchema;
        this.dialectName = dmlOptions.getDialect().dialectName();
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
                        || dmlOptions.getKeyFields().isPresent(),
                "please declare primary key for sink table when query contains update/delete record.");
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        final TypeInformation<RowData> rowDataTypeInformation =
                context.createTypeInformation(tableSchema.toRowDataType());
        final JdbcDynamicOutputFormatBuilder builder = new JdbcDynamicOutputFormatBuilder();

        builder.setJdbcOptions(jdbcOptions);
        builder.setJdbcDmlOptions(dmlOptions);
        builder.setJdbcExecutionOptions(executionOptions);
        builder.setRowDataTypeInfo(rowDataTypeInformation);
        builder.setFieldDataTypes(tableSchema.getFieldDataTypes());
        return OutputFormatProvider.of(builder.build());
    }

    /**
     * 子类实现
     * @return
     */
    @Override
    public abstract DynamicTableSink copy();

    /**
     * 子类实现
     * @return
     */
    @Override
    public abstract String asSummaryString();

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof DtJdbcDynamicTableSink)) {
            return false;
        }
        DtJdbcDynamicTableSink that = (DtJdbcDynamicTableSink) o;
        return Objects.equals(jdbcOptions, that.jdbcOptions)
                && Objects.equals(executionOptions, that.executionOptions)
                && Objects.equals(dmlOptions, that.dmlOptions)
                && Objects.equals(tableSchema, that.tableSchema)
                && Objects.equals(dialectName, that.dialectName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jdbcOptions, executionOptions, dmlOptions, tableSchema, dialectName);
    }
}
