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

package com.dtstack.flinkx.connector.jdbc;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.logical.RowType;

import com.dtstack.flinkx.converter.AbstractRowConverter;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
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
     *
     * @return True if the dialect can be applied on the given jdbc url.
     */
    boolean canHandle(String url);

    /**
     * Get converter that convert jdbc object and Flink internal object each other.
     *
     * @param rowType the given row type
     *
     * @return a row converter for the database
     */
    AbstractRowConverter getRowConverter(RowType rowType);

    /**
     * ColumnConverter
     *
     * @return a row converter for the database
     */
    AbstractRowConverter getRowConverter(List<String> typeList);

    /**
     * Check if this dialect instance support a specific data type in table schema.
     *
     * @param schema the table schema.
     *
     * @throws ValidationException in case of the table schema contains unsupported type.
     */
    default void validate(TableSchema schema) throws ValidationException {
    }

    /**
     * @return the default driver class name, if user not configure the driver class name, then will
     *         use this one.
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
     * @param tableName
     * @param fieldNames
     * @param uniqueKeyFields
     * @param allReplace Whether to replace the original value with a null value ，if true replace else not replace
     *
     * @return None if dialect does not support upsert statement, the writer will degrade to the use
     *         of select + update/insert, this performance is poor.
     */
    default Optional<String> getUpsertStatement(
            String tableName, String[] fieldNames, String[] uniqueKeyFields, boolean allReplace) {
        return Optional.empty();
    }

    default Optional<String> getReplaceStatement(
            String tableName, String[] fieldNames) {
        return Optional.empty();
    }

    /**
     * 构造查询表结构的sql语句
     *
     * @param tableName 要查询的表名称
     *
     * @return 查询sql
     */
    String getSqlQueryFields(String tableName);

    /**
     * 给表名加引号
     *
     * @param table 表名
     *
     * @return "table"
     */
    String quoteTable(String table);

    /**
     * 获取左引号
     *
     * @return 引号
     */
    String getStartQuote();

    /**
     * 获取右引号
     *
     * @return 引号
     */
    String getEndQuote();


    /** Get row exists statement by condition fields. Default use SELECT. */
    default String getRowExistsStatement(String tableName, String[] conditionFields) {
        String fieldExpressions =
                Arrays.stream(conditionFields)
                        .map(f -> format("%s = :%s", quoteIdentifier(f), f))
                        .collect(Collectors.joining(" AND "));
        return "SELECT 1 FROM " + quoteIdentifier(tableName) + " WHERE " + fieldExpressions;
    }

    /** Get insert into statement. */
    default String getInsertIntoStatement(String tableName, String[] fieldNames) {
        String columns =
                Arrays.stream(fieldNames)
                        .map(this::quoteIdentifier)
                        .collect(Collectors.joining(", "));
        String placeholders =
                Arrays.stream(fieldNames).map(f -> ":" + f).collect(Collectors.joining(", "));
        return "INSERT INTO "
                + quoteIdentifier(tableName)
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
            String tableName, String[] fieldNames, String[] conditionFields) {
        String setClause =
                Arrays.stream(fieldNames)
                        .map(f -> format("%s = :%s", quoteIdentifier(f), f))
                        .collect(Collectors.joining(", "));
        String conditionClause =
                Arrays.stream(conditionFields)
                        .map(f -> format("%s = :%s", quoteIdentifier(f), f))
                        .collect(Collectors.joining(" AND "));
        return "UPDATE "
                + quoteIdentifier(tableName)
                + " SET "
                + setClause
                + " WHERE "
                + conditionClause;
    }

    /**
     * Get delete one row statement by condition fields, default not use limit 1, because limit 1 is
     * a sql dialect.
     */
    default String getDeleteStatement(String tableName, String[] conditionFields) {
        String conditionClause =
                Arrays.stream(conditionFields)
                        .map(f -> format("%s = :%s", quoteIdentifier(f), f))
                        .collect(Collectors.joining(" AND "));
        return "DELETE FROM " + quoteIdentifier(tableName) + " WHERE " + conditionClause;
    }

    /** Get select fields statement by condition fields. Default use SELECT. */
    default String getSelectFromStatement(
            String tableName, String[] selectFields, String[] conditionFields) {
        String selectExpressions =
                Arrays.stream(selectFields)
                        .map(this::quoteIdentifier)
                        .collect(Collectors.joining(", "));
        String fieldExpressions =
                Arrays.stream(conditionFields)
                        .map(f -> format("%s = ?", quoteIdentifier(f), f))
                        .collect(Collectors.joining(" AND "));
        return "SELECT "
                + selectExpressions
                + " FROM "
                + quoteIdentifier(tableName)
                + (conditionFields.length > 0 ? " WHERE " + fieldExpressions : "");
    }

    /**
     * Get select fields statement by condition fields. Default use SELECT.
     * @param schemaName
     * @param tableName
     * @param customSql
     * @param selectFields
     * @param where
     * @return
     */
    String getSelectFromStatement(String schemaName, String tableName, String customSql, String[] selectFields, String where);
}
