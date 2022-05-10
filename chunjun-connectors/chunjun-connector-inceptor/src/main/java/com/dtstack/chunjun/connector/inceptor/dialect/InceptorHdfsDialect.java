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

package com.dtstack.chunjun.connector.inceptor.dialect;

import com.dtstack.chunjun.connector.inceptor.converter.InceptorHdfsRawTypeConverter;
import com.dtstack.chunjun.connector.inceptor.converter.InceptorHdfsRowConverter;
import com.dtstack.chunjun.connector.inceptor.sink.InceptorHdfsOutputFormatBuilder;
import com.dtstack.chunjun.connector.inceptor.source.InceptorHdfsInputFormatBuilder;
import com.dtstack.chunjun.connector.jdbc.sink.JdbcOutputFormatBuilder;
import com.dtstack.chunjun.connector.jdbc.source.JdbcInputFormatBuilder;
import com.dtstack.chunjun.connector.jdbc.statement.FieldNamedPreparedStatement;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.converter.RawTypeConverter;

import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import io.vertx.core.json.JsonArray;

import java.sql.ResultSet;
import java.util.Arrays;
import java.util.stream.Collectors;

/** @author liuliu 2022/3/4 */
public class InceptorHdfsDialect extends InceptorDialect {
    @Override
    public AbstractRowConverter<ResultSet, JsonArray, FieldNamedPreparedStatement, LogicalType>
            getRowConverter(RowType rowType) {
        return new InceptorHdfsRowConverter(rowType);
    }

    @Override
    public RawTypeConverter getRawTypeConverter() {
        return InceptorHdfsRawTypeConverter::apply;
    }

    public String getInsertPartitionIntoStatement(
            String schema,
            String tableName,
            String partitionKey,
            String partiitonValue,
            String[] fieldNames) {
        String columns =
                Arrays.stream(fieldNames)
                        .map(this::quoteIdentifier)
                        .collect(Collectors.joining(", "));
        String placeholders =
                Arrays.stream(fieldNames).map(f -> ":" + f).collect(Collectors.joining(", "));
        return "INSERT INTO "
                + buildTableInfoWithSchema(schema, tableName)
                + " PARTITION "
                + " ( "
                + quoteIdentifier(partitionKey)
                + "="
                + "'"
                + partiitonValue
                + "'"
                + " ) "
                + "("
                + columns
                + ")"
                + " SELECT "
                + placeholders
                + "  FROM  SYSTEM.DUAL";
    }

    @Override
    public String appendJdbcTransactionType(String jdbcUrl) {
        return jdbcUrl;
    }

    @Override
    public JdbcInputFormatBuilder getInputFormatBuilder() {
        return new InceptorHdfsInputFormatBuilder();
    }

    @Override
    public JdbcOutputFormatBuilder getOutputFormatBuilder() {
        return new InceptorHdfsOutputFormatBuilder();
    }
}
