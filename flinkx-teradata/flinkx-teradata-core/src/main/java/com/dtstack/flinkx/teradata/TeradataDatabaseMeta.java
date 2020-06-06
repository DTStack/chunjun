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

package com.dtstack.flinkx.teradata;


import com.dtstack.flinkx.enums.EDatabaseType;
import com.dtstack.flinkx.rdb.BaseDatabaseMeta;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The class of TeraData database prototype
 *
 * Company: www.dtstack.com
 * @author wuhui
 */
public class TeradataDatabaseMeta extends BaseDatabaseMeta {

    @Override
    public EDatabaseType getDatabaseType() {
        return EDatabaseType.TeraData;
    }

    @Override
    public String getDriverClass() {
        return "com.teradata.jdbc.TeraDriver";
    }

    @Override
    public String getSqlQueryFields(String tableName) {
        return "SELECT * FROM " + tableName + " QUALIFY SUM(1) OVER (ROWS UNBOUNDED PRECEDING) BETWEEN 0 AND 0";
    }

    @Override
    public String getSqlQueryColumnFields(List<String> column, String table) {
        return "SELECT " + quoteColumns(column) + " FROM " + quoteTable(table) + " QUALIFY SUM(1) OVER (ROWS UNBOUNDED" +
                " PRECEDING) BETWEEN 0 AND 0";
    }

    @Override
    public String getStartQuote() {
        return "\"";
    }

    @Override
    public String getEndQuote() {
        return "\"";
    }

    @Override
    public String quoteValue(String value, String column) {
        return String.format("\"%s\" as %s",value,column);
    }

    @Override
    public String getReplaceStatement(List<String> column, List<String> fullColumn, String table, Map<String,List<String>> updateKey) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getUpsertStatement(List<String> column, String table, Map<String,List<String>> updateKey) {
        throw new UnsupportedOperationException();
    }

    private String makeUpdatePart (List<String> column) {
        List<String> updateList = new ArrayList<>();
        for(String col : column) {
            String quotedCol = quoteColumn(col);
            updateList.add(quotedCol + "=values(" + quotedCol + ")");
        }
        return StringUtils.join(updateList, ",");
    }

    @Override
    public String getSplitFilter(String columnName) {
        return String.format("%s mod ${N} = ${M}", getStartQuote() + columnName + getEndQuote());
    }

    @Override
    public String getSplitFilterWithTmpTable(String tmpTable, String columnName){
        return String.format("%s.%s mod ${N} = ${M}", tmpTable, getStartQuote() + columnName + getEndQuote());
    }

    @Override
    public String getRowNumColumn(String orderBy) {
        throw new RuntimeException("Not support row_number function");
    }

    private String makeValues(int nCols) {
        return "(" + StringUtils.repeat("?", ",", nCols) + ")";
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
