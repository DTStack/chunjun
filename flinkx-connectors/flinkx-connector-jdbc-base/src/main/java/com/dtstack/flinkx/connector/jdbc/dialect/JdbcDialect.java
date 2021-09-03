/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.flinkx.connector.jdbc.dialect;

import com.dtstack.flinkx.conf.FlinkxCommonConf;
import com.dtstack.flinkx.connector.jdbc.converter.JdbcColumnConverter;
import com.dtstack.flinkx.connector.jdbc.converter.JdbcRowConverter;
import com.dtstack.flinkx.connector.jdbc.source.JdbcInputSplit;
import com.dtstack.flinkx.connector.jdbc.statement.FieldNamedPreparedStatement;
import com.dtstack.flinkx.connector.jdbc.util.JdbcUtil;
import com.dtstack.flinkx.converter.AbstractRowConverter;
import com.dtstack.flinkx.converter.RawTypeConverter;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import io.vertx.core.json.JsonArray;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.lang.String.format;

/** Handle the SQL dialect of jdbc driver. */
public interface JdbcDialect extends Serializable {

    /**
     * Get the name of jdbc dialect.
     *
     * @return the dialect name.
     */
    String dialectName();

    /**
     * Check if this dialect instance can handle a certain jdbc url.
     *
     * @param url the jdbc url.
     * @return True if the dialect can be applied on the given jdbc url.
     */
    boolean canHandle(String url);

    /** get jdbc RawTypeConverter */
    RawTypeConverter getRawTypeConverter();

    /**
     * Get converter that convert jdbc object and Flink internal object each other.
     *
     * @param rowType the given row type
     * @return a row converter for the database
     */
    default AbstractRowConverter<ResultSet, JsonArray, FieldNamedPreparedStatement, LogicalType>
            getRowConverter(RowType rowType) {
        return new JdbcRowConverter(rowType);
    }

    /**
     * ColumnConverter
     *
     * @return a row converter for the database
     */
    default AbstractRowConverter<ResultSet, JsonArray, FieldNamedPreparedStatement, LogicalType>
            getColumnConverter(RowType rowType) {
        return getColumnConverter(rowType, null);
    }

    /**
     * ColumnConverter
     *
     * @return a row converter for the database
     */
    default AbstractRowConverter<ResultSet, JsonArray, FieldNamedPreparedStatement, LogicalType>
            getColumnConverter(RowType rowType, FlinkxCommonConf commonConf) {
        return new JdbcColumnConverter(rowType, commonConf);
    }

    /**
     * Check if this dialect instance support a specific data type in table schema.
     *
     * @param schema the table schema.
     * @throws ValidationException in case of the table schema contains unsupported type.
     */
    default void validate(TableSchema schema) throws ValidationException {}

    /**
     * @return the default driver class name, if user not configure the driver class name, then will
     *     use this one.
     */
    default Optional<String> defaultDriverName() {
        return Optional.empty();
    }

    /**
     * Quotes the identifier. This is used to put quotes around the identifier in case the column
     * name is a reserved keyword, or in case it contains characters that require quotes (e.g.
     * space). Default using double quotes {@code "} to quote.
     */
    default String quoteIdentifier(String identifier) {
        return "\"" + identifier + "\"";
    }

    /**
     * Get dialect upsert statement, the database has its own upsert syntax, such as Mysql using
     * DUPLICATE KEY UPDATE, and PostgresSQL using ON CONFLICT... DO UPDATE SET..
     *
     * @param tableName table-name
     * @param fieldNames array of field-name
     * @param uniqueKeyFields array of unique-key
     * @param allReplace Whether to replace the original value with a null value ，if true replace
     *     else not replace
     * @return None if dialect does not support upsert statement, the writer will degrade to the use
     *     of select + update/insert, this performance is poor.
     */
    default Optional<String> getUpsertStatement(
            String schema,
            String tableName,
            String[] fieldNames,
            String[] uniqueKeyFields,
            boolean allReplace) {
        return Optional.empty();
    }

    default Optional<String> getReplaceStatement(
            String schema, String tableName, String[] fieldNames) {
        return Optional.empty();
    }

    /** 构造查询表结构的sql语句 */
    default String getSqlQueryFields(String schema, String tableName) {
        return "SELECT * FROM " + buildTableInfoWithSchema(schema, tableName) + " LIMIT 0";
    }

    /** quote table. Table may like 'schema.table' or 'table' */
    default String quoteTable(String table) {
        String[] strings = table.split("\\.");
        StringBuilder sb = new StringBuilder(64);

        for (int i = 0; i < strings.length; ++i) {
            if (i != 0) {
                sb.append(".");
            }

            sb.append(quoteIdentifier(strings[i]));
        }

        return sb.toString();
    }

    /** Get row exists statement by condition fields. Default use SELECT. */
    default String getRowExistsStatement(
            String schema, String tableName, String[] conditionFields) {
        String fieldExpressions =
                Arrays.stream(conditionFields)
                        .map(f -> format("%s = ?", quoteIdentifier(f)))
                        .collect(Collectors.joining(" AND "));
        return "SELECT 1 FROM "
                + buildTableInfoWithSchema(schema, tableName)
                + " WHERE "
                + fieldExpressions;
    }

    /** Get insert into statement. */
    default String getInsertIntoStatement(String schema, String tableName, String[] fieldNames) {
        String columns =
                Arrays.stream(fieldNames)
                        .map(this::quoteIdentifier)
                        .collect(Collectors.joining(", "));
        String placeholders =
                Arrays.stream(fieldNames).map(f -> ":" + f).collect(Collectors.joining(", "));
        return "INSERT INTO "
                + buildTableInfoWithSchema(schema, tableName)
                + "("
                + columns
                + ")"
                + " VALUES ("
                + placeholders
                + ")";
    }

    /**
     * Get update one row statement by condition fields, default not use limit 1, because limit 1 is
     * a sql dialect.
     */
    default String getUpdateStatement(
            String schema, String tableName, String[] fieldNames, String[] conditionFields) {
        String setClause =
                Arrays.stream(fieldNames)
                        .map(f -> format("%s = ?", quoteIdentifier(f)))
                        .collect(Collectors.joining(", "));
        String conditionClause =
                Arrays.stream(conditionFields)
                        .map(f -> format("%s = ?", quoteIdentifier(f)))
                        .collect(Collectors.joining(" AND "));
        return "UPDATE "
                + buildTableInfoWithSchema(schema, tableName)
                + " SET "
                + setClause
                + " WHERE "
                + conditionClause;
    }

    /**
     * Get delete one row statement by condition fields, default not use limit 1, because limit 1 is
     * a sql dialect.
     */
    default String getDeleteStatement(String schema, String tableName, String[] conditionFields) {
        String conditionClause =
                Arrays.stream(conditionFields)
                        .map(f -> format("%s = :%s", quoteIdentifier(f), f))
                        .collect(Collectors.joining(" AND "));
        return "DELETE FROM "
                + buildTableInfoWithSchema(schema, tableName)
                + " WHERE "
                + conditionClause;
    }

    /** Get select fields statement by condition fields. Default use SELECT. */
    default String getSelectFromStatement(
            String schema, String tableName, String[] selectFields, String[] conditionFields) {
        String selectExpressions =
                Arrays.stream(selectFields)
                        .map(this::quoteIdentifier)
                        .collect(Collectors.joining(", "));
        String fieldExpressions =
                Arrays.stream(conditionFields)
                        .map(f -> format("%s = ?", quoteIdentifier(f)))
                        .collect(Collectors.joining(" AND "));
        return "SELECT "
                + selectExpressions
                + " FROM "
                + buildTableInfoWithSchema(schema, tableName)
                + (conditionFields.length > 0 ? " WHERE " + fieldExpressions : "");
    }

    /** Get select fields statement by condition fields. Default use SELECT. */
    default String getSelectFromStatement(
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
            sql.append(selectExpressions)
                    .append(" FROM ")
                    .append(buildTableInfoWithSchema(schemaName, tableName));
        }

        sql.append(" WHERE ");
        if (StringUtils.isNotBlank(where)) {
            sql.append(where);
        } else {
            sql.append(" 1=1 ");
        }

        return sql.toString();
    }

    /** Get select fields and customFields statement by condition fields. Default use SELECT. */
    default String getSelectFromStatement(
            String schemaName,
            String tableName,
            String customSql,
            String[] selectFields,
            String[] selectCustomFields,
            String where) {
        String selectExpressions =
                Arrays.stream(selectFields)
                        .map(this::quoteIdentifier)
                        .collect(Collectors.joining(", "));
        if (Objects.nonNull(selectCustomFields) && selectCustomFields.length > 0) {
            selectExpressions = selectExpressions + ", " + String.join(", ", selectCustomFields);
        }
        StringBuilder sql = new StringBuilder(128);
        sql.append("SELECT ");
        if (StringUtils.isNotBlank(customSql)) {
            sql.append("* FROM (")
                    .append(customSql)
                    .append(") ")
                    .append(JdbcUtil.TEMPORARY_TABLE_NAME);
        } else {
            sql.append(selectExpressions)
                    .append(" FROM ")
                    .append(buildTableInfoWithSchema(schemaName, tableName));
        }
        sql.append(" WHERE ");
        if (StringUtils.isNotBlank(where)) {
            sql.append(where);
        } else {
            sql.append(" 1=1 ");
        }
        return sql.toString();
    }

    /** build table-info with schema-info and table-name, like 'schema-info.table-name' */
    default String buildTableInfoWithSchema(String schema, String tableName) {
        if (StringUtils.isNotBlank(schema)) {
            return quoteIdentifier(schema) + "." + quoteIdentifier(tableName);
        } else {
            return quoteIdentifier(tableName);
        }
    }

    /** build row number field */
    default String getRowNumColumn(String orderBy) {
        throw new UnsupportedOperationException("Not support row_number function");
    }

    /** get splitKey aliasName */
    default String getRowNumColumnAlias() {
        return "FLINKX_ROWNUM";
    }

    /** build split filter by range, like 'id >=0 and id < 100' */
    default String getSplitRangeFilter(JdbcInputSplit split, String splitPkName) {
        StringBuilder sql = new StringBuilder(128);
        if (StringUtils.isNotBlank(split.getStartLocationOfSplit())) {
            sql.append(quoteIdentifier(splitPkName))
                    .append(" >= ")
                    .append(split.getStartLocationOfSplit());
        }

        if (StringUtils.isNotBlank(split.getEndLocationOfSplit())) {
            if (sql.length() > 0) {
                sql.append(" AND ");
            }
            sql.append(quoteIdentifier(splitPkName))
                    .append(" < ")
                    .append(split.getEndLocationOfSplit());
        }

        return sql.toString();
    }

    /** build split filter by mod, like ' mod(id,2) = 1' */
    default String getSplitModFilter(JdbcInputSplit split, String splitPkName) {
        return String.format(
                " mod(%s, %s) = %s",
                quoteIdentifier(splitPkName), split.getTotalNumberOfSplits(), split.getMod());
    }
}
