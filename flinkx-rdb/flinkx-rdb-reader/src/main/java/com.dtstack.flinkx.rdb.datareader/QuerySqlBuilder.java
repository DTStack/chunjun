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

import com.dtstack.flinkx.enums.EDatabaseType;
import com.dtstack.flinkx.rdb.DatabaseInterface;
import com.dtstack.flinkx.reader.MetaColumn;
import com.dtstack.flinkx.util.StringUtil;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * @author jiangbo
 * @date 2019/8/16
 */
public class QuerySqlBuilder {

    private static final String CUSTOM_SQL_TEMPLATE = "select * from (%s) %s";
    private static final String TEMPORARY_TABLE_NAME = "flinkx_tmp";
    private static final String INCREMENT_FILTER_PLACEHOLDER = "${incrementFilter}";
    private static final String RESTORE_FILTER_PLACEHOLDER = "${restoreFilter}";
    private static final String SQL_SPLIT_WITH_ROW_NUM = "SELECT * FROM (%s) tmp WHERE %s";
    private static final String ROW_NUM_COLUMN_ALIAS = "FLINKX_ROWNUM";

    private DatabaseInterface databaseInterface;
    private String table;
    private List<MetaColumn> metaColumns;
    private String splitKey;
    private String customFilter;
    private String customSql;
    private boolean isSplitByKey;
    private boolean isIncrement;
    private String incrementColumn;
    private String restoreColumn;
    private boolean isRestore;
    private String orderByColumn;

    public QuerySqlBuilder(JdbcDataReader reader) {
        databaseInterface = reader.databaseInterface;
        table = reader.table;
        metaColumns = reader.metaColumns;
        splitKey = reader.splitKey;
        customFilter = reader.customSql;
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

    private String buildQuerySql(){
        List<String> selectColumns = buildSelectColumns(databaseInterface, metaColumns);
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

        if(EDatabaseType.PostgreSQL.equals(databaseInterface.getDatabaseType())){
            sb.append(buildOrderSql());
        }

        if(isSplitByKey && splitWithRowNum){
            return String.format(SQL_SPLIT_WITH_ROW_NUM, sb.toString(), databaseInterface.getSplitFilter(ROW_NUM_COLUMN_ALIAS));
        } else {
            return sb.toString();
        }
    }

    private String buildOrderSql(){
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

    private String buildQuerySqlWithCustomSql(){
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

        return querySql.toString();
    }

    private static List<String> buildSelectColumns(DatabaseInterface databaseInterface, List<MetaColumn> metaColumns){
        List<String> selectColumns = new ArrayList<>();
        if(metaColumns.size() == 1 && "*".equals(metaColumns.get(0).getName())){
            selectColumns.add("*");
        } else {
            for (MetaColumn metaColumn : metaColumns) {
                if (metaColumn.getValue() != null){
                    selectColumns.add(databaseInterface.quoteValue(metaColumn.getValue(),metaColumn.getName()));
                } else {
                    selectColumns.add(databaseInterface.quoteColumn(metaColumn.getName()));
                }
            }
        }

        return selectColumns;
    }

    private static boolean addRowNumColumn(DatabaseInterface databaseInterface, List<String> selectColumns, boolean isSplitByKey,String splitKey){
        if(!isSplitByKey || !splitKey.contains("(")){
            return false;
        }

        String orderBy = splitKey.substring(splitKey.indexOf("(")+1, splitKey.indexOf(")"));
        selectColumns.add(databaseInterface.getRowNumColumn(orderBy));

        return true;
    }
}
