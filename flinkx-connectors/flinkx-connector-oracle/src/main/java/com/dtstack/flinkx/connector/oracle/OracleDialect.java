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

package com.dtstack.flinkx.connector.oracle;

import com.dtstack.flinkx.connector.jdbc.JdbcDialect;
import com.dtstack.flinkx.connector.jdbc.util.JdbcUtil;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * @program: flinkx
 * @author: wuren
 * @create: 2021/03/17
 */
public class OracleDialect implements JdbcDialect {

    @Override
    public String dialectName() {
        return "ORACLE";
    }

    @Override
    public boolean canHandle(String url) {
        return url.startsWith("jdbc:oracle:thin:");
    }

    @Override
    public Optional<String> defaultDriverName() {
        return Optional.of("oracle.jdbc.OracleDriver");
    }

    @Override
    public String quoteIdentifier(String identifier) {
        return "\"" + identifier + "\"";
    }


    @Override
    public String quoteTable(String table) {
        String[] parts = table.split("\\.");
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < parts.length; ++i) {
            if (i != 0) {
                sb.append(".");
            }
            sb.append(quoteIdentifier(parts[i]));
        }
        return sb.toString();
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

    public String queryOracleMetaDataStatement(
            String schemaName,
            String tableName,
            String[] selectFields) {
        String selectExpressions =
                Arrays.stream(selectFields)
                        .collect(Collectors.joining("','","'","'"));
        StringBuilder sql = new StringBuilder(128);
        sql.append("SELECT DATA_PRECISION,DATA_SCALE,COLUMN_NAME,DATA_TYPE FROM ALL_TAB_COLUMNS ");
        sql.append("WHERE TABLE_NAME = '").append(tableName).append("'").append(" AND OWNER = '")
                .append(schemaName).append("'")
                .append(" AND COLUMN_NAME in (").append(selectExpressions).append(")");
        return sql.toString();
    }
}
