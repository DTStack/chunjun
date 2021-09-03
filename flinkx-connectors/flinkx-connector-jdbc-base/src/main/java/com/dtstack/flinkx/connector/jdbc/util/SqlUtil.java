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

package com.dtstack.flinkx.connector.jdbc.util;

import com.dtstack.flinkx.connector.jdbc.conf.JdbcConf;
import com.dtstack.flinkx.connector.jdbc.dialect.JdbcDialect;
import com.dtstack.flinkx.connector.jdbc.source.JdbcInputSplit;
import com.dtstack.flinkx.constants.ConstantValue;
import com.dtstack.flinkx.enums.ColumnType;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.function.Function;

public class SqlUtil {
    protected final Logger LOG = LoggerFactory.getLogger(getClass());

    public static String buildQuerySplitRangeSql(JdbcConf jdbcConf, JdbcDialect jdbcDialect) {
        /** 构建where条件 * */
        String whereFilter = "";
        if (StringUtils.isNotBlank(jdbcConf.getWhere())) {
            whereFilter = " WHERE " + jdbcConf.getWhere();
        }

        String querySplitRangeSql;
        if (StringUtils.isNotEmpty(jdbcConf.getCustomSql())) {
            querySplitRangeSql =
                    String.format(
                            "SELECT max(%s.%s) as max_value, min(%s.%s) as min_value FROM ( %s ) %s %s",
                            JdbcUtil.TEMPORARY_TABLE_NAME,
                            jdbcDialect.quoteIdentifier(jdbcConf.getSplitPk()),
                            JdbcUtil.TEMPORARY_TABLE_NAME,
                            jdbcDialect.quoteIdentifier(jdbcConf.getSplitPk()),
                            jdbcConf.getCustomSql(),
                            JdbcUtil.TEMPORARY_TABLE_NAME,
                            whereFilter);

        } else {
            // rowNum字段作为splitKey
            if (addRowNumColumn(jdbcConf.getSplitPk())) {
                StringBuilder customTableBuilder =
                        new StringBuilder(128)
                                .append("SELECT ")
                                .append(getRowNumColumn(jdbcConf.getSplitPk(), jdbcDialect))
                                .append(" FROM ")
                                .append(
                                        jdbcDialect.buildTableInfoWithSchema(
                                                jdbcConf.getSchema(), jdbcConf.getTable()))
                                .append(whereFilter);

                querySplitRangeSql =
                        String.format(
                                "SELECT max(%s) as max_value, min(%s) as min_value FROM (%s)tmp",
                                jdbcDialect.quoteIdentifier(jdbcDialect.getRowNumColumnAlias()),
                                jdbcDialect.quoteIdentifier(jdbcDialect.getRowNumColumnAlias()),
                                customTableBuilder);
            } else {
                querySplitRangeSql =
                        String.format(
                                "SELECT max(%s) as max_value, min(%s) as min_value FROM %s %s",
                                jdbcDialect.quoteIdentifier(jdbcConf.getSplitPk()),
                                jdbcDialect.quoteIdentifier(jdbcConf.getSplitPk()),
                                jdbcDialect.buildTableInfoWithSchema(
                                        jdbcConf.getSchema(), jdbcConf.getTable()),
                                whereFilter);
            }
        }
        return querySplitRangeSql;
    }

    /** create querySql for inputSplit * */
    public static String buildQuerySqlBySplit(
            JdbcConf jdbcConf,
            JdbcDialect jdbcDialect,
            List<String> whereList,
            List<String> columnNameList,
            JdbcInputSplit jdbcInputSplit) {
        // customSql为空 且 splitPk是ROW_NUMBER()
        boolean flag =
                StringUtils.isBlank(jdbcConf.getCustomSql())
                        && SqlUtil.addRowNumColumn(jdbcConf.getSplitPk());

        String splitFilter = null;
        if (jdbcInputSplit.getTotalNumberOfSplits() > 1) {
            String splitColumn;
            if (flag) {
                splitColumn = jdbcDialect.getRowNumColumnAlias();
            } else {
                splitColumn = jdbcConf.getSplitPk();
            }
            splitFilter =
                    buildSplitFilterSql(
                            jdbcConf.getSplitStrategy(), jdbcDialect, jdbcInputSplit, splitColumn);
        }

        String querySql;
        if (flag) {
            String whereSql = String.join(" AND ", whereList.toArray(new String[0]));
            String tempQuerySql =
                    jdbcDialect.getSelectFromStatement(
                            jdbcConf.getSchema(),
                            jdbcConf.getTable(),
                            jdbcConf.getCustomSql(),
                            columnNameList.toArray(new String[0]),
                            Lists.newArrayList(
                                            SqlUtil.getRowNumColumn(
                                                    jdbcConf.getSplitPk(), jdbcDialect))
                                    .toArray(new String[0]),
                            whereSql);

            // like 'SELECT * FROM (SELECT "id", "name", rownum as FLINKX_ROWNUM FROM "table" WHERE
            // "id"  >  2) flinkx_tmp WHERE FLINKX_ROWNUM >= 1  and FLINKX_ROWNUM < 10 '
            querySql =
                    jdbcDialect.getSelectFromStatement(
                            jdbcConf.getSchema(),
                            jdbcConf.getTable(),
                            tempQuerySql,
                            columnNameList.toArray(new String[0]),
                            splitFilter);
        } else {
            if (StringUtils.isNotEmpty(splitFilter)) {
                whereList.add(splitFilter);
            }
            String whereSql = String.join(" AND ", whereList.toArray(new String[0]));
            // like 'SELECT * FROM (SELECT "id", "name" FROM "table") flinkx_tmp WHERE id >= 1 and
            // id <10 '
            querySql =
                    jdbcDialect.getSelectFromStatement(
                            jdbcConf.getSchema(),
                            jdbcConf.getTable(),
                            jdbcConf.getCustomSql(),
                            columnNameList.toArray(new String[0]),
                            whereSql);
        }
        return querySql;
    }

    /**
     * 构造过滤条件SQL
     *
     * @param operator 比较符
     * @param location 比较的值
     * @param columnName 字段名称
     * @param columnType 字段类型
     * @param isPolling 是否是轮询任务
     * @return
     */
    public static String buildFilterSql(
            String customSql,
            String operator,
            String location,
            String columnName,
            String columnType,
            boolean isPolling,
            Function<Long, String> function) {
        StringBuilder sql = new StringBuilder(64);
        if (StringUtils.isNotEmpty(customSql)) {
            sql.append(JdbcUtil.TEMPORARY_TABLE_NAME).append(".");
        }
        sql.append(columnName).append(" ").append(operator).append(" ");
        if (isPolling) {
            // 轮询任务使用占位符
            sql.append("?");
        } else {
            sql.append(SqlUtil.buildLocation(columnType, location, function));
        }

        return sql.toString();
    }

    /**
     * buildLocation
     *
     * @param columnType
     * @param location
     * @return
     */
    public static String buildLocation(
            String columnType, String location, Function<Long, String> function) {
        if (ColumnType.isTimeType(columnType)) {
            return function.apply(Long.parseLong(location));
        } else if (ColumnType.isNumberType(columnType)) {
            return location;
        } else {
            return "'" + location + "'";
        }
    }

    /**
     * build order sql
     *
     * @param sortRule
     * @return
     */
    public static String buildOrderSql(
            JdbcConf jdbcConf, JdbcDialect jdbcDialect, String sortRule) {
        String column;
        // 增量任务
        if (jdbcConf.isIncrement() && !jdbcConf.isPolling()) {
            column = jdbcConf.getIncreColumn();
        } else {
            column = jdbcConf.getOrderByColumn();
        }
        return StringUtils.isBlank(column)
                ? ""
                : String.format(" ORDER BY %s %s", jdbcDialect.quoteIdentifier(column), sortRule);
    }

    /* 是否添加自定义函数column 作为分片key ***/
    public static boolean addRowNumColumn(String splitKey) {
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
        if ("range".equalsIgnoreCase(splitStrategy)) {
            return jdbcDialect.getSplitRangeFilter(jdbcInputSplit, splitColumn);
        } else {
            return jdbcDialect.getSplitModFilter(jdbcInputSplit, splitColumn);
        }
    }
}
