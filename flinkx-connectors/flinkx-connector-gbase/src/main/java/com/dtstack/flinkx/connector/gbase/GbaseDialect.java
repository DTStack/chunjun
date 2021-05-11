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

package com.dtstack.flinkx.connector.gbase;

import com.dtstack.flinkx.connector.jdbc.JdbcDialect;
import com.dtstack.flinkx.util.DtStringUtil;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * @author tiezhu
 * @since 2021/5/8 4:11 下午
 */
public class GbaseDialect implements JdbcDialect {

    private static final String GBASE_QUOTATION_MASK = "`";

    @Override
    public String dialectName() {
        return "Gbase";
    }

    @Override
    public boolean canHandle(String url) {
        return url.startsWith("jdbc:gbase:");
    }

    @Override
    public Optional<String> defaultDriverName() {
        return Optional.of("com.gbase.jdbc.Driver");
    }

    @Override
    public String getSqlQueryFields(String tableName) {
        return "SELECT * FROM " + tableName + " LIMIT 0";
    }

    @Override
    public Optional<String> getUpsertStatement(
            String tableName, String[] fieldNames, String[] uniqueKeyFields, boolean allReplace) {
        return JdbcDialect.super.getUpsertStatement(
                tableName, fieldNames, uniqueKeyFields, allReplace);
    }

    public Optional<String> getUpsertStatement(
            String schema,
            String tableName,
            String[] fieldNames,
            String[] uniqueKeyFields,
            boolean allReplace) {
        tableName = DtStringUtil.getTableFullPath(schema, tableName);
        StringBuilder sb = new StringBuilder();
        sb.append("MERGE INTO ")
                .append(tableName)
                .append(" T1 USING ")
                .append("(")
                .append(buildValuesStatement(fieldNames))
                .append(") T2 (")
                .append(String.join(", ", fieldNames))
                .append(") ON (")
                .append(buildConnectionConditions(uniqueKeyFields))
                .append(") ");

        String updateSql = buildUpdateConnection(fieldNames, uniqueKeyFields, allReplace);

        if (StringUtils.isNotEmpty(updateSql)) {
            sb.append(" WHEN MATCHED THEN UPDATE SET ").append(updateSql);
        }

        sb.append(" WHEN NOT MATCHED THEN " + "INSERT (")
                .append(
                        Arrays.stream(fieldNames)
                                .map(this::quoteIdentifier)
                                .collect(Collectors.joining(",")))
                .append(") VALUES (")
                .append(
                        Arrays.stream(fieldNames)
                                .map(col -> "T2." + quoteIdentifier(col))
                                .collect(Collectors.joining(",")))
                .append(")");
        return Optional.of(sb.toString());
    }

    /**
     * build T1."A"=T2."A" or T1."A"=nvl(T2."A",T1."A")
     *
     * @param fieldNames
     * @param uniqueKeyFields
     * @param allReplace
     * @return
     */
    private String buildUpdateConnection(
            String[] fieldNames, String[] uniqueKeyFields, boolean allReplace) {
        List<String> uniqueKeyList = Arrays.asList(uniqueKeyFields);
        return Arrays.stream(fieldNames)
                .filter(col -> !uniqueKeyList.contains(col))
                .map(col -> buildConnectString(allReplace, col))
                .collect(Collectors.joining(","));
    }

    private String buildConnectString(boolean allReplace, String col) {
        return allReplace
                ? quoteIdentifier("T1")
                        + "."
                        + quoteIdentifier(col)
                        + " = "
                        + quoteIdentifier("T2")
                        + "."
                        + quoteIdentifier(col)
                : quoteIdentifier("T1")
                        + "."
                        + quoteIdentifier(col)
                        + " =NVL("
                        + quoteIdentifier("T2")
                        + "."
                        + quoteIdentifier(col)
                        + ","
                        + quoteIdentifier("T1")
                        + "."
                        + quoteIdentifier(col)
                        + ")";
    }

    private String buildConnectionConditions(String[] uniqueKeyFields) {
        return Arrays.stream(uniqueKeyFields)
                .map(col -> "T1." + quoteIdentifier(col) + "=T2." + quoteIdentifier(col))
                .collect(Collectors.joining(","));
    }

    /**
     * build sql part e.g: VALUES('1001','zs','sss')
     *
     * @param column destination column
     * @return e.g: VALUES('1001','zs','sss')
     */
    public String buildValuesStatement(String[] column) {
        StringBuilder sb = new StringBuilder("VALUES(");
        String collect = Arrays.stream(column).map(col -> " ? ").collect(Collectors.joining(", "));

        return sb.append(collect).append(")").toString();
    }

    /**
     * 给表名加引号
     *
     * @param table 表名
     * @return "table"
     */
    @Override
    public String quoteTable(String table) {
        String[] strings = table.split("\\.");
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < strings.length; ++i) {
            if (i != 0) {
                sb.append(".");
            }

            sb.append(quoteIdentifier(strings[i]));
        }

        return sb.toString();
    }

    @Override
    public String quoteIdentifier(String identifier) {
        if (identifier.startsWith(GBASE_QUOTATION_MASK)
                && identifier.endsWith(GBASE_QUOTATION_MASK)) {
            return identifier;
        }
        return GBASE_QUOTATION_MASK + identifier + GBASE_QUOTATION_MASK;
    }
}
