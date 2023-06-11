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

package com.dtstack.chunjun.connector.gbase.dialect;

import com.dtstack.chunjun.connector.gbase.converter.GbaseRawTypeConverter;
import com.dtstack.chunjun.connector.jdbc.dialect.JdbcDialect;
import com.dtstack.chunjun.converter.RawTypeMapper;

import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class GbaseDialect implements JdbcDialect {

    private static final String GBASE_QUOTATION_MASK = "`";

    @Override
    public String dialectName() {
        return "GBase";
    }

    @Override
    public boolean canHandle(String url) {
        return url.startsWith("jdbc:gbase:");
    }

    @Override
    public RawTypeMapper getRawTypeConverter() {
        return GbaseRawTypeConverter::apply;
    }

    @Override
    public Optional<String> defaultDriverName() {
        return Optional.of("com.gbase.jdbc.Driver");
    }

    /** build select sql , such as (SELECT :A "A",? "B" FROM DUAL) */
    public String buildDualQueryStatement(String[] column) {
        StringBuilder sb = new StringBuilder("SELECT count(1),");
        String placeholders =
                Arrays.stream(column)
                        .map(f -> ":" + f + " as " + quoteIdentifier(f))
                        .collect(Collectors.joining(", "));
        sb.append(placeholders);

        return sb.toString();
    }

    @Override
    public Optional<String> getUpsertStatement(
            String schema,
            String tableName,
            String[] fieldNames,
            String[] uniqueKeyFields,
            boolean allReplace) {
        tableName = buildTableInfoWithSchema(schema, tableName);
        StringBuilder mergeIntoSql = new StringBuilder(64);
        mergeIntoSql
                .append("MERGE INTO ")
                .append(tableName)
                .append(" T1 USING (")
                .append(buildDualQueryStatement(fieldNames))
                .append(" FROM ")
                .append(tableName)
                .append(" limit 1 ")
                .append(") T2 ON (")
                .append(buildEqualConditions(uniqueKeyFields))
                .append(") ");

        String updateSql = buildUpdateConnection(fieldNames, uniqueKeyFields, allReplace);

        if (StringUtils.isNotEmpty(updateSql)) {
            mergeIntoSql.append(" WHEN MATCHED THEN UPDATE SET ");
            mergeIntoSql.append(updateSql);
        }

        mergeIntoSql
                .append(" WHEN NOT MATCHED THEN ")
                .append("INSERT (")
                .append(
                        Arrays.stream(fieldNames)
                                .map(this::quoteIdentifier)
                                .collect(Collectors.joining(", ")))
                .append(") VALUES (")
                .append(
                        Arrays.stream(fieldNames)
                                .map(col -> "T2." + quoteIdentifier(col))
                                .collect(Collectors.joining(", ")))
                .append(")");

        return Optional.of(mergeIntoSql.toString());
    }

    /** build T1."A"=T2."A" or T1."A"=nvl(T2."A",T1."A") */
    private String buildUpdateConnection(
            String[] fieldNames, String[] uniqueKeyFields, boolean allReplace) {
        List<String> uniqueKeyList = Arrays.asList(uniqueKeyFields);
        return Arrays.stream(fieldNames)
                .filter(col -> !uniqueKeyList.contains(col))
                .map(col -> buildConnectString(allReplace, col))
                .collect(Collectors.joining(","));
    }

    /**
     * Depending on parameter [allReplace] build different sql part. e.g T1."A"=T2."A" or
     * T1."A"=nvl(T2."A",T1."A")
     */
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

    /** build sql part e.g: T1.`A` = T2.`A`, T1.`B` = T2.`B` */
    private String buildEqualConditions(String[] uniqueKeyFields) {
        return Arrays.stream(uniqueKeyFields)
                .map(col -> "T1." + quoteIdentifier(col) + " = T2." + quoteIdentifier(col))
                .collect(Collectors.joining(", "));
    }

    @Override
    public String quoteIdentifier(String identifier) {
        if (identifier.startsWith(GBASE_QUOTATION_MASK)
                && identifier.endsWith(GBASE_QUOTATION_MASK)) {
            return identifier;
        }
        return GBASE_QUOTATION_MASK + identifier + GBASE_QUOTATION_MASK;
    }

    @Override
    public String getRowNumColumn(String orderBy) {
        return "ROWID as " + getRowNumColumnAlias();
    }
}
