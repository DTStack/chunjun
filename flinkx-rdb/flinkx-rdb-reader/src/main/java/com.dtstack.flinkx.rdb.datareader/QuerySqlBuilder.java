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


package com.dtstack.flinkx.rdb.datareader;

import com.dtstack.flinkx.constants.ConstantValue;
import com.dtstack.flinkx.rdb.DatabaseInterface;
import com.dtstack.flinkx.rdb.util.DbUtil;
import com.dtstack.flinkx.reader.MetaColumn;
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

    protected DatabaseInterface databaseInterface;
    protected String table;
    protected List<MetaColumn> metaColumns;
    protected String splitKey;
    protected String customFilter;
    protected String customSql;
    protected boolean isSplitByKey;
    protected boolean isIncrement;
    protected String incrementColumn;
    protected String restoreColumn;
    protected boolean isRestore;
    protected String orderByColumn;

    public QuerySqlBuilder(JdbcDataReader reader) {
        databaseInterface = reader.databaseInterface;
        table = reader.table;
        metaColumns = reader.metaColumns;
        splitKey = reader.splitKey;
        customFilter = reader.where;
        customSql = reader.customSql;
        isSplitByKey = reader.getNumPartitions() > 1 && StringUtils.isNotEmpty(splitKey);
        isIncrement = reader.incrementConfig.isIncrement();
        incrementColumn = reader.incrementConfig.getColumnName();
        isRestore = reader.getRestoreConfig().isRestore();
        restoreColumn = reader.getRestoreConfig().getRestoreColumnName();
        orderByColumn = reader.orderByColumn;
    }

    public QuerySqlBuilder(DatabaseInterface databaseInterface,String table,List<MetaColumn> metaColumns,
                           String splitKey,String customFilter,boolean isSplitByKey,boolean isIncrement,boolean isRestore) {
        this.databaseInterface = databaseInterface;
        this.table = table;
        this.metaColumns = metaColumns;
        this.splitKey = splitKey;
        this.customFilter = customFilter;
        this.isSplitByKey = isSplitByKey;
        this.isIncrement = isIncrement;
        this.isRestore = isRestore;
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
        List<String> selectColumns = DbUtil.buildSelectColumns(databaseInterface, metaColumns);
        boolean splitWithRowNum = addRowNumColumn(databaseInterface, selectColumns, isSplitByKey, splitKey);

        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ").append(StringUtils.join(selectColumns,",")).append(" FROM ");
        sb.append(databaseInterface.quoteTable(table));
        sb.append(" WHERE 1=1 ");

        StringBuilder filter = new StringBuilder();

        if(isSplitByKey && !splitWithRowNum) {
            filter.append(" AND ").append(databaseInterface.getSplitFilter(splitKey));
        }

        if (customFilter != null){
            customFilter = customFilter.trim();
            if (customFilter.length() > 0){
                filter.append(" AND ").append(customFilter);
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
            return String.format(SQL_SPLIT_WITH_ROW_NUM, sb.toString(), databaseInterface.getSplitFilter(ROW_NUM_COLUMN_ALIAS));
        } else {
            return sb.toString();
        }
    }

    protected String buildOrderSql(){
        String column;
        if(isIncrement){
            column = incrementColumn;
        } else if(isRestore){
            column = restoreColumn;
        } else {
            column = orderByColumn;
        }

        return StringUtils.isEmpty(column) ? "" : String.format(" order by %s", column);
    }

    protected String buildQuerySqlWithCustomSql(){
        StringBuilder querySql = new StringBuilder();
        querySql.append(String.format(CUSTOM_SQL_TEMPLATE, customSql, TEMPORARY_TABLE_NAME));
        querySql.append(" WHERE 1=1 ");

        if (isSplitByKey){
            querySql.append(" And ").append(databaseInterface.getSplitFilterWithTmpTable(TEMPORARY_TABLE_NAME, splitKey));
        }

        if(isIncrement){
            querySql.append(" ").append(INCREMENT_FILTER_PLACEHOLDER);
        }

        if(isRestore){
            querySql.append(" ").append(RESTORE_FILTER_PLACEHOLDER);
        }

        if (customFilter != null){
            customFilter = customFilter.trim();
            if (customFilter.length() > 0){
                querySql.append(" AND ").append(customFilter);
            }
        }

        return querySql.toString();
    }

    protected static boolean addRowNumColumn(DatabaseInterface databaseInterface, List<String> selectColumns, boolean isSplitByKey,String splitKey){
        if(!isSplitByKey || !splitKey.contains(ConstantValue.LEFT_PARENTHESIS_SYMBOL)){
            return false;
        }

        String orderBy = splitKey.substring(splitKey.indexOf(ConstantValue.LEFT_PARENTHESIS_SYMBOL)+1, splitKey.indexOf(ConstantValue.RIGHT_PARENTHESIS_SYMBOL));
        selectColumns.add(databaseInterface.getRowNumColumn(orderBy));

        return true;
    }
}
