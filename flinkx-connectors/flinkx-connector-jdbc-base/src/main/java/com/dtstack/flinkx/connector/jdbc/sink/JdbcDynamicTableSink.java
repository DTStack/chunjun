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

package com.dtstack.flinkx.connector.jdbc.sink;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc.internal.options.JdbcDmlOptions;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import com.dtstack.flinkx.connector.jdbc.conf.SinkConnectionConf;
import com.dtstack.flinkx.connector.jdbc.outputformat.JdbcOutputFormat;
import com.dtstack.flinkx.connector.jdbc.outputformat.JdbcOutputFormatBuilder;
import com.dtstack.flinkx.streaming.api.functions.sink.DtOutputFormatSinkFunction;

import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * @author chuixue
 * @create 2021-04-12 14:44
 * @description
 **/
public class JdbcDynamicTableSink implements DynamicTableSink {

    private final SinkConnectionConf connectionConf;
    private final JdbcDialect jdbcDialect;
    private final JdbcExecutionOptions executionOptions;
    private final JdbcDmlOptions dmlOptions;
    private final TableSchema tableSchema;
    private final String dialectName;

    public JdbcDynamicTableSink(
            SinkConnectionConf connectionConf,
            JdbcDialect jdbcDialect,
            JdbcExecutionOptions executionOptions,
            JdbcDmlOptions dmlOptions,
            TableSchema tableSchema) {
        this.connectionConf = connectionConf;
        this.jdbcDialect = jdbcDialect;
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
    public SinkFunctionProvider getSinkRuntimeProvider(Context context) {
        final TypeInformation<RowData> rowDataTypeInformation =
                context.createTypeInformation(tableSchema.toRowDataType());

        // 通过该参数得到类型转换器，将数据库中的字段转成对应的类型
        final RowType rowType = (RowType) tableSchema.toRowDataType().getLogicalType();

        //todo 这里需要被overwrite，传入实际插件的OutputFormat
        JdbcOutputFormatBuilder builder = new JdbcOutputFormatBuilder(new JdbcOutputFormat());
//                .builder()
//                .setJdbcOptions(jdbcOptions)
//                .setColumn(tableSchema.getFieldNames())
//                .setRowType(rowType)
//                .setMode((dmlOptions.getKeyFields().isPresent()
//                        && dmlOptions.getKeyFields().get().length
//                        > 0) ? EWriteMode.UPDATE.name() : EWriteMode.INSERT.name());

        return SinkFunctionProvider.of(new DtOutputFormatSinkFunction(builder.finish()));
    }

    @Override
    public DynamicTableSink copy() {
        return new JdbcDynamicTableSink(
                connectionConf,
                jdbcDialect,
                executionOptions,
                dmlOptions,
                tableSchema);
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
        return Objects.equals(connectionConf, that.connectionConf)
                && Objects.equals(executionOptions, that.executionOptions)
                && Objects.equals(dmlOptions, that.dmlOptions)
                && Objects.equals(tableSchema, that.tableSchema)
                && Objects.equals(dialectName, that.dialectName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(connectionConf, executionOptions, dmlOptions, tableSchema, dialectName);
    }
}
