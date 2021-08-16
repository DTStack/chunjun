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

package com.dtstack.flinkx.connector.phoenix5;

import com.dtstack.flinkx.conf.FlinkxCommonConf;
import com.dtstack.flinkx.connector.jdbc.dialect.JdbcDialect;
import com.dtstack.flinkx.connector.jdbc.statement.FieldNamedPreparedStatement;
import com.dtstack.flinkx.connector.phoenix5.converter.Phoenix5ColumnConverter;
import com.dtstack.flinkx.connector.phoenix5.converter.Phoenix5RawTypeConverter;
import com.dtstack.flinkx.connector.phoenix5.converter.Phoenix5RowConverter;
import com.dtstack.flinkx.converter.AbstractRowConverter;
import com.dtstack.flinkx.converter.RawTypeConverter;

import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import io.vertx.core.json.JsonArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * @author wujuan
 * @version 1.0
 * @date 2021/7/9 16:01 星期五
 * @email wujuan@dtstack.com
 * @company www.dtstack.com
 */
public class Phoenix5Dialect implements JdbcDialect {

    private static final Logger LOG = LoggerFactory.getLogger(Phoenix5Dialect.class);

    @Override
    public String dialectName() {
        return "Phoenix5";
    }

    @Override
    public boolean canHandle(String url) {
        return url.startsWith("jdbc:phoenix:");
    }

    @Override
    public Optional<String> defaultDriverName() {
        return Optional.of("org.apache.phoenix.jdbc.PhoenixDriver");
    }

    /**
     * The sql field of phoenix cannot have backticks(``).
     *
     * @param identifier
     * @return
     */
    @Override
    public String quoteIdentifier(String identifier) {
        return identifier;
    }

    @Override
    public String getInsertIntoStatement(String schema, String tableName, String[] fieldNames) {
        throw new UnsupportedOperationException("phoenix not support update, only support upsert!");
    }

    @Override
    public String getUpdateStatement(
            String schema, String tableName, String[] fieldNames, String[] conditionFields) {
        throw new UnsupportedOperationException("phoenix not support update, only support upsert!");
    }

    /**
     * Phoenix5 upsert query use DUPLICATE KEY UPDATE.
     *
     * <p>NOTE: It requires Phoenix5's primary key to be consistent with pkFields.
     *
     * <p>We don't use REPLACE INTO, if there are other fields, we can keep their previous values.
     */
    @Override
    public Optional<String> getUpsertStatement(
            String schema,
            String tableName,
            String[] fieldNames,
            String[] uniqueKeyFields,
            boolean allReplace) {
        String columns =
                Arrays.stream(fieldNames)
                        .map(this::quoteIdentifier)
                        .collect(Collectors.joining(", "));
        String placeholders =
                Arrays.stream(fieldNames).map(f -> ":" + f).collect(Collectors.joining(", "));
        return Optional.of(
                "UPSERT INTO "
                        + buildTableInfoWithSchema(schema, tableName)
                        + "("
                        + columns
                        + ")"
                        + " VALUES ("
                        + placeholders
                        + ")");
    }

    @Override
    public AbstractRowConverter<ResultSet, JsonArray, FieldNamedPreparedStatement, LogicalType>
            getRowConverter(RowType rowType) {
        return new Phoenix5RowConverter(rowType);
    }

    @Override
    public AbstractRowConverter<ResultSet, JsonArray, FieldNamedPreparedStatement, LogicalType>
            getColumnConverter(RowType rowType, FlinkxCommonConf commonConf) {
        return new Phoenix5ColumnConverter(rowType, commonConf);
    }

    @Override
    public RawTypeConverter getRawTypeConverter() {
        return Phoenix5RawTypeConverter::apply;
    }
}
