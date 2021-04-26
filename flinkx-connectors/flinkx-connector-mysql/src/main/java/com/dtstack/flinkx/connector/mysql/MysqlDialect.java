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

package com.dtstack.flinkx.connector.mysql;

import org.apache.flink.table.types.logical.RowType;

import com.dtstack.flinkx.connector.jdbc.JdbcDialect;
import com.dtstack.flinkx.connector.jdbc.converter.AbstractJdbcRowConverter;
import com.dtstack.flinkx.connector.jdbc.util.JdbcUtil;
import com.dtstack.flinkx.connector.mysql.converter.MysqlRowConverter;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * @program: flinkx
 * @author: wuren
 * @create: 2021/03/17
 **/
public class MysqlDialect implements JdbcDialect {

    @Override
    public String dialectName() {
        return "MySQL";
    }

    @Override
    public boolean canHandle(String url) {
        return url.startsWith("jdbc:mysql:");
    }

    @Override
    public AbstractJdbcRowConverter getRowConverter(RowType rowType) {
        if (rowType != null) {
            return new MysqlRowConverter(rowType);
        } else {
            return new MysqlRowConverter();
        }
    }

    @Override
    public Optional<String> defaultDriverName() {
        return Optional.of("com.mysql.jdbc.Driver");
    }

    @Override
    public String quoteIdentifier(String identifier) {
        return "`" + identifier + "`";
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
            String tableName, String[] fieldNames, String[] uniqueKeyFields, boolean allReplace) {
        String updateClause;
        if (allReplace) {
            updateClause =
                    Arrays.stream(fieldNames)
                            .map(f -> quoteIdentifier(f) + "=VALUES(" + quoteIdentifier(f) + ")")
                            .collect(Collectors.joining(", "));
        } else {
            updateClause = Arrays
                    .stream(fieldNames)
                    .map(f -> quoteIdentifier(f) + "=IFNULL(VALUES(" + quoteIdentifier(f) + "),"
                            + quoteIdentifier(f) + ")")
                    .collect(Collectors.joining(", "));
        }

        return Optional.of(getInsertIntoStatement(tableName, fieldNames)
                + " ON DUPLICATE KEY UPDATE " + updateClause);
    }

    @Override
    public Optional<String> getReplaceStatement(
            String tableName, String[] fieldNames) {
        String columns = Arrays.stream(fieldNames)
                .map(this::quoteIdentifier)
                .collect(Collectors.joining(", "));
        String placeholders = Arrays.stream(fieldNames)
                .map(f -> "?")
                .collect(Collectors.joining(", "));
        return Optional.of("REPLACE INTO " + quoteIdentifier(tableName) +
                "(" + columns + ")" + " VALUES (" + placeholders + ")");
    }

    @Override
    public String getSqlQueryFields(String tableName) {
        return "SELECT * FROM " + tableName + " LIMIT 0";
    }

    @Override
    public String quoteTable(String table) {
        String[] parts = table.split("\\.");
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < parts.length; ++i) {
            if (i != 0) {
                sb.append(".");
            }
            sb.append(getStartQuote() + parts[i] + getEndQuote());
        }
        return sb.toString();
    }

    @Override
    public String getStartQuote() {
        return "\"";
    }

    @Override
    public String getEndQuote() {
        return "\"";
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
        if(StringUtils.isNotBlank(customSql)){
            sql.append("* FROM (")
                    .append(customSql)
                    .append(") ")
                    .append(JdbcUtil.TEMPORARY_TABLE_NAME);
        }else{
            sql.append(selectExpressions)
                    .append(" FROM ");
            if(StringUtils.isNotBlank(schemaName)){
                sql.append(quoteIdentifier(schemaName))
                    .append(" .");
            }
            sql.append(quoteIdentifier(tableName));
        }
        if(StringUtils.isNotBlank(where)){
            sql.append( " WHERE ")
                    .append(where);
        }
        return sql.toString();
    }

    @Override
    public int getFetchSize() {
        return Integer.MIN_VALUE;
    }

    @Override
    public int getQueryTimeout() {
        return 300;
    }
}
