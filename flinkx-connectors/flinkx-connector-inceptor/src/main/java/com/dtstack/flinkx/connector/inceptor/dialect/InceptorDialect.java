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
package com.dtstack.flinkx.connector.inceptor.dialect;

import com.dtstack.flinkx.connector.inceptor.converter.InceptorRawTypeConverter;
import com.dtstack.flinkx.connector.inceptor.converter.InceptorRowConverter;
import com.dtstack.flinkx.connector.jdbc.dialect.JdbcDialect;
import com.dtstack.flinkx.connector.jdbc.statement.FieldNamedPreparedStatement;
import com.dtstack.flinkx.converter.AbstractRowConverter;
import com.dtstack.flinkx.converter.RawTypeConverter;

import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import io.vertx.core.json.JsonArray;

import java.sql.ResultSet;
import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;

public class InceptorDialect implements JdbcDialect {
    @Override
    public String dialectName() {
        return "INCEPTOR";
    }

    @Override
    public boolean canHandle(String url) {
        return url.startsWith("jdbc:hive2:");
    }

    @Override
    public RawTypeConverter getRawTypeConverter() {
        return InceptorRawTypeConverter::apply;
    }

    @Override
    public String quoteIdentifier(String identifier) {
        return "`" + identifier + "`";
    }

    @Override
    public Optional<String> defaultDriverName() {
        return Optional.of("org.apache.hive.jdbc.HiveDriver");
    }

    @Override
    public AbstractRowConverter<ResultSet, JsonArray, FieldNamedPreparedStatement, LogicalType>
            getRowConverter(RowType rowType) {
        return new InceptorRowConverter(rowType);
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
}
