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

package com.dtstack.chunjun.connector.mysql.dialect;

import com.dtstack.chunjun.config.TypeConfig;
import com.dtstack.chunjun.connector.jdbc.conf.TableIdentify;
import com.dtstack.chunjun.connector.jdbc.dialect.JdbcDialect;
import com.dtstack.chunjun.connector.jdbc.statement.FieldNamedPreparedStatement;
import com.dtstack.chunjun.connector.mysql.converter.MysqlRawTypeConverter;
import com.dtstack.chunjun.connector.mysql.converter.MysqlSqlConverter;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.converter.RawTypeMapper;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import io.vertx.core.json.JsonArray;

import java.sql.ResultSet;
import java.util.Arrays;
import java.util.Locale;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

public class MysqlDialect implements JdbcDialect {

    private static final long serialVersionUID = 4607084579671405838L;

    @Override
    public String dialectName() {
        return "MySQL";
    }

    @Override
    public boolean canHandle(String url) {
        return url.startsWith("jdbc:mysql:");
    }

    @Override
    public RawTypeMapper getRawTypeConverter() {
        return MysqlRawTypeConverter::apply;
    }

    @Override
    public Optional<String> defaultDriverName() {
        return Optional.of("com.mysql.jdbc.Driver");
    }

    @Override
    public String quoteIdentifier(String identifier) {
        return "`" + identifier + "`";
    }

    @Override
    public boolean supportUpsert() {
        return true;
    }

    @Override
    public AbstractRowConverter<ResultSet, JsonArray, FieldNamedPreparedStatement, LogicalType>
            getRowConverter(RowType rowType) {
        return new MysqlSqlConverter(rowType);
    }

    /**
     * Mysql upsert query use DUPLICATE KEY UPDATE.
     *
     * <p>NOTE: It requires Mysql's primary key to be consistent with pkFields.
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
    public TableIdentify getTableIdentify(String confSchema, String confTable) {
        return new TableIdentify(confSchema, null, confTable, this::quoteIdentifier, false);
    }

    public Function<Tuple3<String, Integer, Integer>, TypeConfig> typeBuilder() {
        return (typePsTuple -> {
            String columnTypeName = typePsTuple.f0;
            int precision = typePsTuple.f1;
            int scale = typePsTuple.f2;
            if ((columnTypeName.toUpperCase(Locale.ENGLISH).contains("TIMESTAMP")
                            || columnTypeName.toUpperCase(Locale.ENGLISH).contains("DATETIME"))
                    && precision > 19) {
                // "." 还占一个字符长度，需要去掉.
                scale = precision - 19 - 1;
            } else if (columnTypeName.toUpperCase(Locale.ENGLISH).equals("TIME")
                    && precision > 10) {
                // "." 还占一个字符长度，需要去掉.
                scale = precision - 10 - 1;
            }
            TypeConfig typeConfig = TypeConfig.fromString(columnTypeName);
            typeConfig.setPrecision(precision);
            typeConfig.setScale(scale);
            return typeConfig;
        });
    }
}
