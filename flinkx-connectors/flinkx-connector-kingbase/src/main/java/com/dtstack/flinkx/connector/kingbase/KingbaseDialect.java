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

package com.dtstack.flinkx.connector.kingbase;

import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import com.dtstack.flinkx.connector.jdbc.JdbcDialect;
import com.dtstack.flinkx.connector.jdbc.statement.FieldNamedPreparedStatement;
import com.dtstack.flinkx.connector.jdbc.util.JdbcUtil;
import com.dtstack.flinkx.connector.kingbase.converter.KingbaseColumnConverter;
import com.dtstack.flinkx.connector.kingbase.converter.KingbaseRowConverter;
import com.dtstack.flinkx.connector.kingbase.util.KingbaseConstants;
import com.dtstack.flinkx.converter.AbstractRowConverter;
import com.dtstack.flinkx.enums.EDatabaseType;
import io.vertx.core.json.JsonArray;
import org.apache.commons.lang3.StringUtils;

import java.sql.ResultSet;
import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * @description:
 * @program: flinkx-all
 * @author: lany
 * @create: 2021/04/26 22:01
 */
public class KingbaseDialect implements JdbcDialect {

    @Override
    public Optional<String> defaultDriverName() {
        return Optional.of(KingbaseConstants.DRIVER);
    }

    @Override
    public String dialectName() {
        return EDatabaseType.KingBase.name();
    }

    @Override
    public boolean canHandle(String url) {
        return url.startsWith(KingbaseConstants.URL_PREFIX);
    }

    @Override
    public String quoteIdentifier(String identifier) {
        return "" + identifier + "";
    }

    @Override
    public AbstractRowConverter<ResultSet, JsonArray, FieldNamedPreparedStatement, LogicalType> getRowConverter(RowType rowType) {
        return new KingbaseRowConverter(rowType);
    }

    @Override
    public AbstractRowConverter<ResultSet, JsonArray, FieldNamedPreparedStatement, LogicalType> getColumnConverter(RowType rowType) {
        return new KingbaseColumnConverter(rowType);
    }

    @Override
    public Optional<String> getUpsertStatement(String schema, String tableName, String[] fieldNames, String[] uniqueKeyFields, boolean allReplace) {

        String updateClause  =
                Arrays.stream(fieldNames)
                        .map(f -> quoteIdentifier(f) + "=EXCLUDED." + quoteIdentifier(f))
                        .collect(Collectors.joining(", "));

        String uniqueColumns =
                Arrays.stream(uniqueKeyFields)
                .map(this::quoteIdentifier)
                .collect(Collectors.joining(", "));

        return Optional.of(
                getInsertIntoStatement( schema, tableName, fieldNames)
                        + "ON CONFLICT "
                        + uniqueColumns
                        + " DO UPDATE SET  "
                        + updateClause
        );

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
            sql.append(" WHERE ")
                    .append(where);
        }
        return sql.toString();
    }



}
