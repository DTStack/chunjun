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
package com.dtstack.chunjun.connector.oceanbase.dialect;

import com.dtstack.chunjun.config.CommonConfig;
import com.dtstack.chunjun.connector.jdbc.dialect.JdbcDialect;
import com.dtstack.chunjun.connector.jdbc.statement.FieldNamedPreparedStatement;
import com.dtstack.chunjun.connector.oceanbase.converter.OceanbaseRawTypeMapper;
import com.dtstack.chunjun.connector.oceanbase.converter.OceanbaseSyncConverter;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.converter.RawTypeMapper;

import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import io.vertx.core.json.JsonArray;

import java.sql.ResultSet;
import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;

public class OceanbaseDialect implements JdbcDialect {

    private static final long serialVersionUID = -3958547388213339994L;

    private static final String DIALECT_NAME = "OceanBase";
    private static final String DEFAULT_DRIVER_NAME = "com.alipay.oceanbase.jdbc.Driver";

    @Override
    public String dialectName() {
        return DIALECT_NAME;
    }

    @Override
    public boolean canHandle(String url) {
        return url.startsWith("jdbc:oceanbase:");
    }

    @Override
    public Optional<String> defaultDriverName() {
        return Optional.of(DEFAULT_DRIVER_NAME);
    }

    @Override
    public RawTypeMapper getRawTypeConverter() {
        return OceanbaseRawTypeMapper::apply;
    }

    @Override
    public String quoteIdentifier(String identifier) {
        return "`" + identifier + "`";
    }

    @Override
    public Optional<String> getReplaceStatement(
            String schema, String tableName, String[] fieldNames) {
        String columns =
                Arrays.stream(fieldNames)
                        .map(this::quoteIdentifier)
                        .collect(Collectors.joining(", "));
        String placeholders =
                Arrays.stream(fieldNames).map(f -> ":" + f).collect(Collectors.joining(", "));
        return Optional.of(
                "REPLACE INTO "
                        + buildTableInfoWithSchema(schema, tableName)
                        + "("
                        + columns
                        + ")"
                        + " VALUES ("
                        + placeholders
                        + ")");
    }

    @Override
    public Optional<String> getUpsertStatement(
            String schema,
            String tableName,
            String[] fieldNames,
            String[] uniqueKeyFields,
            boolean allReplace) {
        String updateClause;
        if (allReplace) {
            updateClause =
                    Arrays.stream(fieldNames)
                            .map(f -> quoteIdentifier(f) + "=VALUES(" + quoteIdentifier(f) + ")")
                            .collect(Collectors.joining(", "));
        } else {
            updateClause =
                    Arrays.stream(fieldNames)
                            .map(
                                    f ->
                                            quoteIdentifier(f)
                                                    + "=IFNULL(VALUES("
                                                    + quoteIdentifier(f)
                                                    + "),"
                                                    + quoteIdentifier(f)
                                                    + ")")
                            .collect(Collectors.joining(", "));
        }

        return Optional.of(
                getInsertIntoStatement(schema, tableName, fieldNames)
                        + " ON DUPLICATE KEY UPDATE "
                        + updateClause);
    }

    @Override
    public AbstractRowConverter<ResultSet, JsonArray, FieldNamedPreparedStatement, LogicalType>
            getColumnConverter(RowType rowType, CommonConfig commonConfig) {
        return new OceanbaseSyncConverter(rowType, commonConfig);
    }
}
