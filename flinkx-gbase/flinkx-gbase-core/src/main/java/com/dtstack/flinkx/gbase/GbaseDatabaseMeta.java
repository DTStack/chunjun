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


package com.dtstack.flinkx.gbase;

import com.dtstack.flinkx.enums.EDatabaseType;
import com.dtstack.flinkx.rdb.BaseDatabaseMeta;
import org.apache.commons.collections.CollectionUtils;

import java.util.List;
import java.util.Map;

/**
 * @author jiangbo
 * @date 2019/6/9
 */
public class GbaseDatabaseMeta extends BaseDatabaseMeta {

    @Override
    public EDatabaseType getDatabaseType() {
        return EDatabaseType.GBase;
    }

    @Override
    public String getDriverClass() {
        return "com.gbase.jdbc.Driver";
    }

    @Override
    public String getUpsertStatement(List<String> column, String table, Map<String,List<String>> updateKey) {
        if(updateKey == null || updateKey.isEmpty()) {
            return getInsertStatement(column, table);
        }

        List<String> updateColumns = getUpdateColumns(column, updateKey);
        if(CollectionUtils.isEmpty(updateColumns)){
            return "MERGE INTO " + quoteTable(table) + " T1 USING "
                    + "(select " + makeValues(column) + " from " + quoteTable(table) + "  limit 1) T2 ON ("
                    + updateKeySql(updateKey) + ") WHEN NOT MATCHED THEN "
                    + "INSERT (" + quoteColumns(column) + ") VALUES ("
                    + quoteColumns(column, "T2") + ")";
        } else {
            return "MERGE INTO " + quoteTable(table) + " T1 USING "
                    + "(select " + makeValues(column) + " from " + quoteTable(table) + "  limit 1) T2 ON ("
                    + updateKeySql(updateKey) + ") WHEN MATCHED THEN UPDATE SET "
                    + getUpdateSql(updateColumns, "T1", "T2") + " WHEN NOT MATCHED THEN "
                    + "INSERT (" + quoteColumns(column) + ") VALUES ("
                    + quoteColumns(column, "T2") + ")";
        }
    }

    @Override
    protected String makeValues(List<String> column) {
        StringBuilder sb = new StringBuilder();
        for(int i = 0; i < column.size(); ++i) {
            if(i != 0) {
                sb.append(",");
            }
            sb.append("? " + quoteColumn(column.get(i)));
        }
        return sb.toString();
    }

    @Override
    public String getSqlQueryFields(String tableName) {
        return "SELECT * FROM " + tableName + " LIMIT 0";
    }

    @Override
    public String getStartQuote() {
        return "`";
    }

    @Override
    public String getEndQuote() {
        return "`";
    }

    @Override
    public String getSqlQueryColumnFields(List<String> column, String table) {
        return "SELECT " + quoteColumns(column) + " FROM " + quoteTable(table) + " LIMIT 0";
    }

    @Override
    public String quoteValue(String value, String column) {
        return String.format("'%s' as %s",value,column);
    }

    @Override
    public String getSplitFilter(String columnName) {
        return String.format("mod(%s, ${N}) = ${M}", getStartQuote() + columnName + getEndQuote());
    }

    @Override
    public String getSplitFilterWithTmpTable(String tmpTable, String columnName) {
        return String.format("mod(%s.%s, ${N}) = ${M}", tmpTable, getStartQuote() + columnName + getEndQuote());
    }

    @Override
    public String getRowNumColumn(String orderBy) {
        return "ROWID as FLINKX_ROWNUM";
    }

    @Override
    public int getFetchSize(){
        return Integer.MIN_VALUE;
    }

    @Override
    public int getQueryTimeout(){
        return 1000;
    }
}
