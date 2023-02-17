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

package com.dtstack.chunjun.connector.vertica11.dialect;

import com.dtstack.chunjun.config.CommonConfig;
import com.dtstack.chunjun.connector.jdbc.dialect.JdbcDialect;
import com.dtstack.chunjun.connector.jdbc.statement.FieldNamedPreparedStatement;
import com.dtstack.chunjun.connector.vertica11.converter.Vertica11RawTypeConverter;
import com.dtstack.chunjun.connector.vertica11.converter.Vertica11SqlConverter;
import com.dtstack.chunjun.connector.vertica11.converter.Vertica11SyncConverter;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.converter.RawTypeConverter;

import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import io.vertx.core.json.JsonArray;
import org.apache.commons.lang3.StringUtils;

import java.sql.ResultSet;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class Vertica11Dialect implements JdbcDialect {

    private static final long serialVersionUID = -5097687228210606349L;

    private static final String DIALECT_NAME = "VERTICA11";

    private static final String DRIVER_NAME = "com.vertica.jdbc.Driver";

    @Override
    public String dialectName() {
        return DIALECT_NAME;
    }

    @Override
    public boolean canHandle(String url) {
        return url.startsWith("jdbc:vertica:");
    }

    @Override
    public RawTypeConverter getRawTypeConverter() {
        return Vertica11RawTypeConverter::apply;
    }

    @Override
    public Optional<String> defaultDriverName() {
        return Optional.of(DRIVER_NAME);
    }

    @Override
    public String quoteIdentifier(String identifier) {
        return "\"" + identifier + "\"";
    }

    @Override
    public AbstractRowConverter<ResultSet, JsonArray, FieldNamedPreparedStatement, LogicalType>
            getRowConverter(RowType rowType) {
        return new Vertica11SqlConverter(rowType);
    }

    @Override
    public AbstractRowConverter<ResultSet, JsonArray, FieldNamedPreparedStatement, LogicalType>
            getColumnConverter(RowType rowType, CommonConfig commonConfig) {
        return new Vertica11SyncConverter(rowType, commonConfig);
    }

    public Optional<String> getUpsertStatement(
            String schema,
            String tableName,
            String[] fieldNames,
            String[] fieldTypes,
            String[] uniqueKeyFields,
            boolean allReplace) {
        tableName = buildTableInfoWithSchema(schema, tableName);
        StringBuilder mergeIntoSql = new StringBuilder(64);
        mergeIntoSql
                .append("MERGE INTO ")
                .append(tableName)
                .append(" T1 USING (")
                .append(buildDualQueryStatement(fieldNames, fieldTypes))
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

    /** build select sql , such as (SELECT ? "A",? "B" FROM DUAL) */
    public String buildDualQueryStatement(String[] column, String[] types) {
        AtomicInteger index = new AtomicInteger(0);
        StringBuilder sb = new StringBuilder("SELECT ");
        String collect =
                Arrays.stream(column)
                        .map(
                                col ->
                                        "cast(:"
                                                + col
                                                + " as "
                                                + types[index.getAndIncrement()]
                                                + ") "
                                                + quoteIdentifier(col))
                        .collect(Collectors.joining(", "));
        sb.append(collect).append(" FROM DUAL");
        return sb.toString();
    }

    /** build sql part e.g: T1.`A` = T2.`A`, T1.`B` = T2.`B` */
    private String buildEqualConditions(String[] uniqueKeyFields) {
        return Arrays.stream(uniqueKeyFields)
                .map(col -> "T1." + quoteIdentifier(col) + " = T2." + quoteIdentifier(col))
                .collect(Collectors.joining(" and "));
    }

    /** build T1."A"=T2."A" or T1."A"=nvl(T2."A",T1."A") */
    private String buildUpdateConnection(
            String[] fieldNames, String[] uniqueKeyFields, boolean allReplace) {
        List<String> uniqueKeyList = Arrays.asList(uniqueKeyFields);
        return Arrays.stream(fieldNames)
                .filter(col -> !uniqueKeyList.contains(col.toLowerCase()))
                .map(col -> buildConnectString(allReplace, col))
                .collect(Collectors.joining(","));
    }

    /**
     * Depending on parameter [allReplace] build different sql part. e.g T1."A"=T2."A" or
     * T1."A"=nvl(T2."A",T1."A")
     */
    private String buildConnectString(boolean allReplace, String col) {
        return allReplace
                ? quoteIdentifier(col) + " = T2." + quoteIdentifier(col)
                : quoteIdentifier(col) + " =T2." + quoteIdentifier(col);
    }
}
