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

package com.dtstack.chunjun.connector.postgresql.dialect;

import com.dtstack.chunjun.config.CommonConfig;
import com.dtstack.chunjun.connector.jdbc.dialect.JdbcDialect;
import com.dtstack.chunjun.connector.jdbc.statement.FieldNamedPreparedStatement;
import com.dtstack.chunjun.connector.jdbc.util.JdbcUtil;
import com.dtstack.chunjun.connector.postgresql.converter.PostgresqlRawTypeMapper;
import com.dtstack.chunjun.connector.postgresql.converter.PostgresqlSyncConverter;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.converter.RawTypeMapper;

import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import io.vertx.core.json.JsonArray;
import org.apache.commons.lang3.StringUtils;

import java.sql.ResultSet;
import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;

public class PostgresqlDialect implements JdbcDialect {

    private static final long serialVersionUID = 2647649364696033202L;

    private static final String DIALECT_NAME = "PostgreSQL";
    private static final String DRIVER = "org.postgresql.Driver";
    public static final String URL_START = "jdbc:postgresql:";

    protected static final String COPY_SQL_TEMPL =
            "copy %s(%s) from stdin DELIMITER '%s' NULL as '%s'";

    @Override
    public String dialectName() {
        return DIALECT_NAME;
    }

    @Override
    public boolean canHandle(String url) {
        return url.startsWith(URL_START);
    }

    @Override
    public RawTypeMapper getRawTypeConverter() {
        return PostgresqlRawTypeMapper::apply;
    }

    @Override
    public AbstractRowConverter<ResultSet, JsonArray, FieldNamedPreparedStatement, LogicalType>
            getColumnConverter(RowType rowType, CommonConfig commonConfig) {
        return new PostgresqlSyncConverter(rowType, commonConfig);
    }

    @Override
    public Optional<String> defaultDriverName() {
        return Optional.of(DRIVER);
    }

    @Override
    public boolean supportUpsert() {
        return true;
    }

    /** Postgres upsert query. It use ON CONFLICT ... DO UPDATE SET.. to replace into Postgres. */
    @Override
    public Optional<String> getUpsertStatement(
            String schema,
            String tableName,
            String[] fieldNames,
            String[] uniqueKeyFields,
            boolean allReplace) {
        String updateClause;
        String uniqueColumns =
                Arrays.stream(uniqueKeyFields)
                        .map(this::quoteIdentifier)
                        .collect(Collectors.joining(", "));
        updateClause =
                Arrays.stream(fieldNames)
                        .filter(f -> !Arrays.asList(uniqueKeyFields).contains(f))
                        .map(f -> quoteIdentifier(f) + "=EXCLUDED." + quoteIdentifier(f))
                        .collect(Collectors.joining(", "));

        return Optional.of(
                getInsertIntoStatement(schema, tableName, fieldNames)
                        + " ON CONFLICT ("
                        + uniqueColumns
                        + ")"
                        + " DO UPDATE SET "
                        + updateClause);
    }

    @Override
    public String getSelectFromStatement(
            String schemaName,
            String tableName,
            String customSql,
            String[] selectFields,
            String where) {
        String selectExpressions =
                Arrays.stream(selectFields)
                        .map(this::quoteIdentifier)
                        .collect(Collectors.joining(", "));
        StringBuilder sql = new StringBuilder(128);
        sql.append("SELECT ");
        if (StringUtils.isNotBlank(customSql)) {
            sql.append("* FROM (")
                    .append(customSql)
                    .append(") ")
                    .append(JdbcUtil.TEMPORARY_TABLE_NAME);
        } else {
            sql.append(selectExpressions).append(" FROM ");
            if (StringUtils.isNotBlank(schemaName)) {
                sql.append(quoteIdentifier(schemaName)).append(" .");
            }
            sql.append(quoteIdentifier(tableName));
        }

        if (StringUtils.isNotBlank(where)) {
            sql.append(" WHERE ").append(where);
        }

        return sql.toString();
    }

    public String getCopyStatement(
            String schemaName,
            String tableName,
            String[] fields,
            String fieldDelimiter,
            String nullVal) {
        String fieldsExpression =
                Arrays.stream(fields).map(this::quoteIdentifier).collect(Collectors.joining(", "));

        String tableLocation;
        if (schemaName != null && !"".equals(schemaName.trim())) {
            tableLocation = quoteIdentifier(schemaName) + "." + quoteIdentifier(tableName);
        } else {
            tableLocation = quoteIdentifier(tableName);
        }

        return String.format(
                COPY_SQL_TEMPL, tableLocation, fieldsExpression, fieldDelimiter, nullVal);
    }
}
