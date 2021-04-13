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


package com.dtstack.flinkx.connector.jdbc.source;

import com.dtstack.flinkx.conf.FieldConf;
import com.dtstack.flinkx.connector.jdbc.DtJdbcDialect;
import com.dtstack.flinkx.connector.jdbc.conf.JdbcConf;
import com.dtstack.flinkx.connector.jdbc.util.JdbcUtil;
import com.dtstack.flinkx.constants.ConstantValue;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

/**
 * @author jiangbo
 * @date 2019/8/16
 */
public class QuerySqlBuilder {

    protected static final String CUSTOM_SQL_TEMPLATE = "select * from (%s) %s";
    protected static final String TEMPORARY_TABLE_NAME = "flinkx_tmp";
    protected static final String INCREMENT_FILTER_PLACEHOLDER = "${incrementFilter}";
    protected static final String RESTORE_FILTER_PLACEHOLDER = "${restoreFilter}";
    protected static final String SQL_SPLIT_WITH_ROW_NUM = "SELECT * FROM (%s) tmp WHERE %s";
    protected static final String ROW_NUM_COLUMN_ALIAS = "FLINKX_ROWNUM";

    protected JdbcConf jdbcConf;

    protected DtJdbcDialect dtJdbcDialect;
    protected String table;
    protected List<FieldConf> metaColumns;
    protected String splitKey;
    protected String where;
    protected String customSql;
    protected boolean isSplitByKey;
    protected boolean isIncrement;
    protected String incrementColumn;
    protected boolean isRestore;
    protected String orderByColumn;

    public QuerySqlBuilder(JdbcDataSource reader) {
        this.jdbcConf = reader.getJdbcConf();
        this.dtJdbcDialect = reader.getDtJdbcDialect();
        this.table = jdbcConf.getTable();
        this.metaColumns = jdbcConf.getColumn();
        this.splitKey = jdbcConf.getSplitPk();
        this.where = jdbcConf.getWhere();
        this.customSql = jdbcConf.getCustomSql();
        this.isSplitByKey = jdbcConf.getParallelism() > 1 && StringUtils.isNotEmpty(splitKey);
        this.isIncrement = jdbcConf.isIncrement();
        this.incrementColumn = jdbcConf.getIncreColumn();
        this.orderByColumn = jdbcConf.getOrderByColumn();
    }

    public QuerySqlBuilder(DtJdbcDialect dtJdbcDialect, String table, List<FieldConf> metaColumns,
                           String splitKey, String customFilter, boolean isSplitByKey, boolean isIncrement, boolean isRestore) {
        this.dtJdbcDialect = dtJdbcDialect;
        this.table = table;
        this.metaColumns = metaColumns;
        this.splitKey = splitKey;
        this.where = customFilter;
        this.isSplitByKey = isSplitByKey;
        this.isIncrement = isIncrement;
        this.isRestore = isRestore;
    }

    protected static boolean addRowNumColumn(DtJdbcDialect dtJdbcDialect, List<String> selectColumns, boolean isSplitByKey,String splitKey){
        if(!isSplitByKey || !splitKey.contains(ConstantValue.LEFT_PARENTHESIS_SYMBOL)){
            return false;
        }

        String orderBy = splitKey.substring(splitKey.indexOf(ConstantValue.LEFT_PARENTHESIS_SYMBOL)+1, splitKey.indexOf(
                ConstantValue.RIGHT_PARENTHESIS_SYMBOL));
//        selectColumns.add(dtJdbcDialect.getRowNumColumn(orderBy));

        return true;
    }

    public String buildSql(){
        String query;
        if (StringUtils.isNotEmpty(customSql)){
            query = buildQuerySqlWithCustomSql();
        } else {
            query = buildQuerySql();
        }

        return query;
    }

    protected String buildQuerySql(){
        List<String> selectColumns = JdbcUtil.buildSelectColumns(dtJdbcDialect, metaColumns);
        boolean splitWithRowNum = addRowNumColumn(dtJdbcDialect, selectColumns, isSplitByKey, splitKey);

        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ").append(StringUtils.join(selectColumns,",")).append(" FROM ");
//        sb.append(dtJdbcDialect.quoteTable(table));
        sb.append(" WHERE 1=1 ");

        StringBuilder filter = new StringBuilder();

        if(isSplitByKey && !splitWithRowNum) {
//            filter.append(" AND ").append(dtJdbcDialect.getSplitFilter(splitKey));
        }

        if (where != null){
            where = where.trim();
            if (where.length() > 0){
                filter.append(" AND ").append(where);
            }
        }

        if(isIncrement){
            filter.append(" ").append(INCREMENT_FILTER_PLACEHOLDER);
        }

        if(isRestore){
            filter.append(" ").append(RESTORE_FILTER_PLACEHOLDER);
        }

        sb.append(filter);

        if(isSplitByKey && splitWithRowNum){
//            return String.format(SQL_SPLIT_WITH_ROW_NUM, sb.toString(), dtJdbcDialect.getSplitFilter(ROW_NUM_COLUMN_ALIAS));
            return null;
        } else {
            return sb.toString();
        }
    }

    protected String buildQuerySqlWithCustomSql(){
        StringBuilder querySql = new StringBuilder();
        querySql.append(String.format(CUSTOM_SQL_TEMPLATE, customSql, TEMPORARY_TABLE_NAME));
        querySql.append(" WHERE 1=1 ");

        if (isSplitByKey){
//            querySql.append(" And ").append(dtJdbcDialect.getSplitFilterWithTmpTable(TEMPORARY_TABLE_NAME, splitKey));
        }

        if(isIncrement){
            querySql.append(" ").append(INCREMENT_FILTER_PLACEHOLDER);
        }

        if(isRestore){
            querySql.append(" ").append(RESTORE_FILTER_PLACEHOLDER);
        }

        if (where != null){
            where = where.trim();
            if (where.length() > 0){
                querySql.append(" AND ").append(where);
            }
        }

        return querySql.toString();
    }
}
