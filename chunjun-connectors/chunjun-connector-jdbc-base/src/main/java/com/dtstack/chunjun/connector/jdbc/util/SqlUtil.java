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

package com.dtstack.chunjun.connector.jdbc.util;

import com.dtstack.chunjun.connector.jdbc.config.JdbcConfig;
import com.dtstack.chunjun.connector.jdbc.dialect.JdbcDialect;
import com.dtstack.chunjun.connector.jdbc.source.JdbcInputSplit;
import com.dtstack.chunjun.constants.ConstantValue;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SqlUtil {

    private static final Pattern DOLLAR_QUOTE =
            Pattern.compile("\\$(?:[A-Za-z_][A-Za-z_0-9]*)?\\$");

    /** Prepare a single custom query for embedding in a derived table. */
    public static String prepareCustomSql(String sql) {
        if (StringUtils.isBlank(sql)) {
            return sql;
        }
        List<Integer> terminators = new ArrayList<>();
        int offset = 0;
        while (offset < sql.length()) {
            char current = sql.charAt(offset);
            char next = offset + 1 < sql.length() ? sql.charAt(offset + 1) : 0;
            if (Character.isWhitespace(current)) {
                offset++;
            } else if ((current == '-' && next == '-')
                    || (current == '#' && (next == 0 || Character.isWhitespace(next)))) {
                while (offset < sql.length()
                        && sql.charAt(offset) != '\n'
                        && sql.charAt(offset) != '\r') {
                    offset++;
                }
            } else if (current == '/' && next == '*') {
                int depth = 1;
                offset += 2;
                while (offset + 1 < sql.length() && depth > 0) {
                    if (sql.charAt(offset) == '/' && sql.charAt(offset + 1) == '*') {
                        depth++;
                        offset += 2;
                    } else if (sql.charAt(offset) == '*' && sql.charAt(offset + 1) == '/') {
                        depth--;
                        offset += 2;
                    } else {
                        offset++;
                    }
                }
                if (depth != 0) {
                    return sql + '\n';
                }
            } else if (current == '\'' || current == '"' || current == '`' || current == '[') {
                char closing = current == '[' ? ']' : current;
                int end = quotedEnd(sql, offset + 1, closing);
                if (end < 0) {
                    return sql + '\n';
                }
                terminators.clear();
                offset = end;
            } else if (current == '$') {
                Matcher matcher = DOLLAR_QUOTE.matcher(sql).region(offset, sql.length());
                if (matcher.lookingAt()) {
                    String delimiter = matcher.group();
                    int end = sql.indexOf(delimiter, matcher.end());
                    if (end < 0) {
                        return sql + '\n';
                    }
                    terminators.clear();
                    offset = end + delimiter.length();
                } else {
                    terminators.clear();
                    offset++;
                }
            } else {
                if (current == ';') {
                    terminators.add(offset);
                } else {
                    terminators.clear();
                }
                offset++;
            }
        }
        StringBuilder normalized = new StringBuilder(sql);
        for (int i = terminators.size() - 1; i >= 0; i--) {
            normalized.deleteCharAt(terminators.get(i));
        }
        // A line comment at EOF must not consume the derived table's closing parenthesis.
        return normalized.append('\n').toString();
    }

    private static int quotedEnd(String sql, int offset, char closing) {
        while (offset < sql.length()) {
            char current = sql.charAt(offset++);
            if (current == closing) {
                if (offset < sql.length() && sql.charAt(offset) == closing) {
                    offset++;
                } else {
                    return offset;
                }
            } else if (current == '\\' && (closing == '\'' || closing == '"')) {
                offset++;
            }
        }
        return -1;
    }

    public static String buildQuerySplitRangeSql(JdbcConfig jdbcConfig, JdbcDialect jdbcDialect) {
        // 构建where条件
        String whereFilter = "";
        if (StringUtils.isNotBlank(jdbcConfig.getWhere())) {
            whereFilter = " WHERE " + jdbcConfig.getWhere();
        }

        String querySplitRangeSql;
        if (StringUtils.isNotEmpty(jdbcConfig.getCustomSql())) {
            querySplitRangeSql =
                    String.format(
                            "SELECT min(%s.%s) as min_value,max(%s.%s) as max_value FROM ( %s ) %s %s",
                            JdbcUtil.TEMPORARY_TABLE_NAME,
                            jdbcDialect.quoteIdentifier(jdbcConfig.getSplitPk()),
                            JdbcUtil.TEMPORARY_TABLE_NAME,
                            jdbcDialect.quoteIdentifier(jdbcConfig.getSplitPk()),
                            prepareCustomSql(jdbcConfig.getCustomSql()),
                            JdbcUtil.TEMPORARY_TABLE_NAME,
                            whereFilter);

        } else {
            // rowNum字段作为splitKey
            if (isRowNumSplitKey(jdbcConfig.getSplitPk())) {
                String customTableBuilder =
                        "SELECT "
                                + getRowNumColumn(jdbcConfig.getSplitPk(), jdbcDialect)
                                + " FROM "
                                + jdbcDialect.buildTableInfoWithSchema(
                                        jdbcConfig.getSchema(), jdbcConfig.getTable())
                                + whereFilter;

                querySplitRangeSql =
                        String.format(
                                "SELECT min(%s) as min_value ,max(%s) as max_value FROM (%s)tmp",
                                jdbcDialect.quoteIdentifier(jdbcDialect.getRowNumColumnAlias()),
                                jdbcDialect.quoteIdentifier(jdbcDialect.getRowNumColumnAlias()),
                                customTableBuilder);
            } else {
                querySplitRangeSql =
                        String.format(
                                "SELECT min(%s) as min_value,max(%s) as max_value FROM %s %s",
                                jdbcDialect.quoteIdentifier(jdbcConfig.getSplitPk()),
                                jdbcDialect.quoteIdentifier(jdbcConfig.getSplitPk()),
                                jdbcDialect.buildTableInfoWithSchema(
                                        jdbcConfig.getSchema(), jdbcConfig.getTable()),
                                whereFilter);
            }
        }
        return querySplitRangeSql;
    }

    /** create querySql for inputSplit * */
    public static String buildQuerySqlBySplit(
            JdbcConfig jdbcConfig,
            JdbcDialect jdbcDialect,
            List<String> whereList,
            List<String> columnNameList,
            JdbcInputSplit jdbcInputSplit) {
        // customSql为空 且 splitPk是ROW_NUMBER()
        boolean flag =
                StringUtils.isBlank(jdbcConfig.getCustomSql())
                        && SqlUtil.isRowNumSplitKey(jdbcConfig.getSplitPk());

        String splitFilter = null;
        if (jdbcInputSplit.getTotalNumberOfSplits() > 1) {
            String splitColumn;
            if (flag) {
                splitColumn = jdbcDialect.getRowNumColumnAlias();
            } else {
                splitColumn = jdbcConfig.getSplitPk();
            }
            splitFilter =
                    buildSplitFilterSql(
                            jdbcInputSplit.getSplitStrategy(),
                            jdbcDialect,
                            jdbcInputSplit,
                            splitColumn);
        }

        String querySql;
        if (flag) {
            String whereSql = String.join(" AND ", whereList.toArray(new String[0]));
            String tempQuerySql =
                    jdbcDialect.getSelectFromStatement(
                            jdbcConfig.getSchema(),
                            jdbcConfig.getTable(),
                            jdbcConfig.getCustomSql(),
                            columnNameList.toArray(new String[0]),
                            Lists.newArrayList(
                                            SqlUtil.getRowNumColumn(
                                                    jdbcConfig.getSplitPk(), jdbcDialect))
                                    .toArray(new String[0]),
                            whereSql);

            // like 'SELECT * FROM (SELECT "id", "name", rownum as CHUNJUN_ROWNUM FROM "table" WHERE
            // "id"  >  2) chunjun_tmp WHERE CHUNJUN_ROWNUM >= 1  and CHUNJUN_ROWNUM < 10 '
            querySql =
                    jdbcDialect.getSelectFromStatement(
                            jdbcConfig.getSchema(),
                            jdbcConfig.getTable(),
                            tempQuerySql,
                            columnNameList.toArray(new String[0]),
                            splitFilter);
        } else {
            if (StringUtils.isNotEmpty(splitFilter)) {
                whereList.add(splitFilter);
            }
            String whereSql = String.join(" AND ", whereList.toArray(new String[0]));
            // like 'SELECT * FROM (SELECT "id", "name" FROM "table") chunjun_tmp WHERE id >= 1 and
            // id <10 '
            querySql =
                    jdbcDialect.getSelectFromStatement(
                            jdbcConfig.getSchema(),
                            jdbcConfig.getTable(),
                            jdbcConfig.getCustomSql(),
                            columnNameList.toArray(new String[0]),
                            whereSql);
        }
        return querySql;
    }

    public static String buildOrderSql(
            String originalSql, JdbcConfig jdbcConf, JdbcDialect jdbcDialect, String sortRule) {
        String column;
        // 增量任务
        if (jdbcConf.isIncrement() && jdbcConf.isOrderBy() && !originalSql.contains("ORDER BY")) {
            column = jdbcConf.getIncreColumn();
        } else {
            column = jdbcConf.getOrderByColumn();
        }

        String additional =
                StringUtils.isBlank(column)
                        ? ""
                        : String.format(
                                " ORDER BY %s %s", jdbcDialect.quoteIdentifier(column), sortRule);
        return originalSql + additional;
    }

    /* 是否添加自定义函数column 作为分片key ***/
    public static boolean isRowNumSplitKey(String splitKey) {
        return StringUtils.isNotBlank(splitKey)
                && splitKey.contains(ConstantValue.LEFT_PARENTHESIS_SYMBOL);
    }

    /** 获取分片key rownum * */
    public static String getRowNumColumn(String splitKey, JdbcDialect jdbcDialect) {
        String orderBy =
                splitKey.substring(
                        splitKey.indexOf(ConstantValue.LEFT_PARENTHESIS_SYMBOL) + 1,
                        splitKey.indexOf(ConstantValue.RIGHT_PARENTHESIS_SYMBOL));
        return jdbcDialect.getRowNumColumn(orderBy);
    }

    /**
     * build splitSql
     *
     * @param jdbcInputSplit
     * @param splitColumn
     * @return
     */
    public static String buildSplitFilterSql(
            String splitStrategy,
            JdbcDialect jdbcDialect,
            JdbcInputSplit jdbcInputSplit,
            String splitColumn) {
        StringBuilder sql = new StringBuilder("(");
        if ("range".equalsIgnoreCase(splitStrategy)) {
            sql.append(jdbcDialect.getSplitRangeFilter(jdbcInputSplit, splitColumn));
        } else {
            sql.append(jdbcDialect.getSplitModFilter(jdbcInputSplit, splitColumn));
        }
        if (jdbcInputSplit.getSplitNumber() == 0) {
            sql.append(" OR ").append(jdbcDialect.quoteIdentifier(splitColumn)).append(" IS NULL");
        }
        sql.append(")");
        return sql.toString();
    }
}
