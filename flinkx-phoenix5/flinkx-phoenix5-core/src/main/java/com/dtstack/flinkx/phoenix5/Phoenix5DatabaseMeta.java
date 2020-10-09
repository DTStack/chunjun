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

package com.dtstack.flinkx.phoenix5;

import com.dtstack.flinkx.constants.ConstantValue;
import com.dtstack.flinkx.enums.EDatabaseType;
import com.dtstack.flinkx.rdb.BaseDatabaseMeta;
import com.dtstack.flinkx.rdb.util.DbUtil;
import com.dtstack.flinkx.reader.MetaColumn;
import org.apache.commons.lang.StringUtils;

import java.util.List;
import java.util.Map;

/**
 * The class of Phoenix prototype
 *
 * Company: www.dtstack.com
 * @author wuhui
 */
public class Phoenix5DatabaseMeta extends BaseDatabaseMeta {

    @Override
    public EDatabaseType getDatabaseType() {
        return EDatabaseType.Phoenix;
    }

    @Override
    public String getDriverClass() {
        return "org.apache.phoenix.jdbc.PhoenixDriver";
    }

    @Override
    public String getSqlQueryFields(String tableName) {
        return "SELECT * FROM " + tableName + " LIMIT 0";
    }

    @Override
    public String getSqlQueryColumnFields(List<String> column, String table) {
        return "SELECT " + quoteColumns(column) + " FROM " + quoteTable(table) + " LIMIT 0";
    }

    /**
     * phoenix查询表结构SQL
     * @param metaColumns 字段信息
     * @param table schema.table
     * @return
     */
    public String getSqlWithLimit1(List<MetaColumn> metaColumns, String table) {
        String columnStr;
        List<String> column = DbUtil.buildSelectColumns(this, metaColumns);
        if(column.size() == 1 && ConstantValue.STAR_SYMBOL.equals(column.get(0))){
            columnStr = ConstantValue.STAR_SYMBOL;
        }else{
            columnStr = quoteColumns(column);
        }
        return new StringBuilder(256)
                .append("SELECT ")
                .append(columnStr)
                .append(" FROM ")
                .append(quoteTable(table))
                .append(" LIMIT 1")
                .toString();
    }

    @Override
    public String getStartQuote() {
        // 对于字段名和表名的quote得用双引号，对于字段值为字符串的得用单引号表示常量
        return "";
    }

    @Override
    public String getEndQuote() {
        return "";
    }

    @Override
    public String quoteValue(String value, String column) {
        return String.format("\"%s\" as %s",value,column);
    }

    @Override
    public String getInsertStatement(List<String> column, String table) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getReplaceStatement(List<String> column, List<String> fullColumn, String table, Map<String,List<String>> updateKey) {
        // phoenix只支持upsert插入
        throw new UnsupportedOperationException();
    }

    @Override
    public String getUpsertStatement(List<String> column, String table, Map<String,List<String>> updateKey) {
        return "UPSERT INTO " + quoteTable(table)
                + " (" + quoteColumns(column) + ") values ("
                + StringUtils.repeat("?", ",", column.size()) + ")";
    }

    @Override
    public String getSplitFilter(String columnName) {
        // phoenix不支持mod，只支持%取余
        return String.format("%s %% ${N} = ${M}", getStartQuote() + columnName + getEndQuote());
    }

    @Override
    public String getSplitFilterWithTmpTable(String tmpTable, String columnName){
        return String.format("%s.%s %% ${N} = ${M}", tmpTable, getStartQuote() + columnName + getEndQuote());
    }

    @Override
    public String getRowNumColumn(String orderBy) {
        throw new RuntimeException("Not support row_number function");
    }

    @Override
    protected String makeValues(List<String> column) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getFetchSize(){
        return 1000;
    }

    @Override
    public int getQueryTimeout(){
        return 1000;
    }
}
