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

package com.dtstack.flinkx.connector.clickhouse;

import com.dtstack.flinkx.connector.jdbc.JdbcDialect;
import com.dtstack.flinkx.connector.jdbc.util.JdbcUtil;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * @program: flinkx
 * @author: xiuzhu
 * @create: 2021/05/08
 */
public class ClickhouseDialect implements JdbcDialect {

    @Override
    public String dialectName() {
        return "ClickHouse";
    }

    @Override
    public boolean canHandle(String url) {
        return url.startsWith("jdbc:clickhouse:");
    }

    @Override
    public Optional<String> defaultDriverName() {
        return Optional.of("ru.yandex.clickhouse.ClickHouseDriver");
    }

    @Override
    public String getUpdateStatement(String schema, String tableName, String[] fieldNames, String[] conditionFields) {
        throw new RuntimeException("Clickhouse does not support update sql");
    }

    @Override
    public Optional<String> getReplaceStatement(String schema, String tableName, String[] fieldNames) {
        throw new RuntimeException("Clickhouse does not support replace sql");
    }

    @Override
    public String getDeleteStatement(String schema, String tableName, String[] conditionFields) {
        throw new RuntimeException("Clickhouse does not support delete sql");
    }

    @Override
    public String getSelectFromStatement(String schemaName, String tableName, String customSql, String[] selectFields, String where) {
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
}
