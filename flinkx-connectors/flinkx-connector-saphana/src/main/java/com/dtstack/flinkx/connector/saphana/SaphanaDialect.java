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

package com.dtstack.flinkx.connector.saphana;

import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import com.dtstack.flinkx.connector.jdbc.JdbcDialect;
import com.dtstack.flinkx.connector.jdbc.statement.FieldNamedPreparedStatement;
import com.dtstack.flinkx.connector.saphana.converter.SaphanaColumnConverter;
import com.dtstack.flinkx.connector.saphana.converter.SaphanaRowConverter;
import com.dtstack.flinkx.converter.AbstractRowConverter;
import com.dtstack.flinkx.enums.EDatabaseType;
import io.vertx.core.json.JsonArray;
import org.apache.commons.lang3.StringUtils;

import java.sql.ResultSet;
import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * company www.dtstack.com
 *
 * @author jier
 */
public class SaphanaDialect implements JdbcDialect {

    @Override
    public String dialectName() {
        return EDatabaseType.SapHana.name();
    }

    @Override
    public boolean canHandle(String url) {
        return url.startsWith("jdbc:sap:");
    }

    @Override
    public Optional<String> defaultDriverName() {
        return Optional.of("com.sap.db.jdbc.Driver");
    }

    @Override
    public Optional<String> getReplaceStatement(
            String schema,
            String tableName,
            String[] fieldNames) {
        String quoteTable = quoteTable(tableName);
        String selectExpressions =
                Arrays.stream(fieldNames)
                        .map(this::quoteIdentifier)
                        .collect(Collectors.joining(", "));
        String placeholder =
                "(" + StringUtils.repeat("?", ",", fieldNames.length) + ")";
        String replaceSql = "REPLACE INTO " + quoteTable
                + " (" + selectExpressions + ") values "
                + placeholder;
        return Optional.of(replaceSql);
    }

    @Override
    public Optional<String> getUpsertStatement(
            String schema,
            String tableName,
            String[] fieldNames,
            String[] uniqueKeyFields,
            boolean allReplace) {
        String quoteTable = quoteTable(tableName);
        Stream<String> fieldStream = Arrays.stream(fieldNames);
        String selectExpressions =
                fieldStream
                        .map(this::quoteIdentifier)
                        .collect(Collectors.joining(", "));
        String placeholder = "(" + StringUtils.repeat("?", ",", fieldNames.length) + ")";
        String updatePart = fieldStream.map(e->{
            String quotedCol = quoteIdentifier(e);
            return quotedCol + "=values(" + quotedCol + ")";
        }).collect(Collectors.joining(","));
        String upsertSql =  "INSERT INTO " + quoteTable
                + " (" + selectExpressions + ") values "
                + placeholder
                + " ON DUPLICATE KEY UPDATE "
                + updatePart;
        return Optional.of(upsertSql);
    }

    @Override
    public AbstractRowConverter<ResultSet, JsonArray, FieldNamedPreparedStatement, LogicalType> getRowConverter(
            RowType rowType) {
        return new SaphanaRowConverter(rowType);
    }

    @Override
    public AbstractRowConverter<ResultSet, JsonArray, FieldNamedPreparedStatement, LogicalType> getColumnConverter(
            RowType rowType) {
        return new SaphanaColumnConverter(rowType);
    }



    @Override
    public String getRowNumColumn(String orderBy) {
        return "rownum as FLINKX_ROWNUM";
    }
}
