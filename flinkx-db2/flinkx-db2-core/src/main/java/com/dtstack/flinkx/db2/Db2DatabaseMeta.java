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

package com.dtstack.flinkx.db2;

import com.dtstack.flinkx.enums.EDatabaseType;
import com.dtstack.flinkx.rdb.BaseDatabaseMeta;

import java.util.List;

/**
 * The class of DB2 database prototype
 *
 * @author jiangbo
 * @date 2018/11/19
 */
public class Db2DatabaseMeta extends BaseDatabaseMeta {

    @Override
    protected String makeReplaceValues(List<String> column, List<String> fullColumn){
        String replaceValues = super.makeReplaceValues(column,fullColumn);
        return "(select " + replaceValues + " from SYSIBM.SYSDUMMY1)";
    }

    @Override
    protected String makeValues(List<String> column) {
        StringBuilder sb = new StringBuilder("SELECT ");
        for(int i = 0; i < column.size(); ++i) {
            if(i != 0) {
                sb.append(",");
            }
            sb.append("? " + quoteColumn(column.get(i)));
        }
        sb.append(" FROM SYSIBM.SYSDUMMY1");
        return sb.toString();
    }

    @Override
    public String getSqlQueryFields(String tableName) {
        return "SELECT * FROM " + tableName + " FETCH FIRST  1 ROWS ONLY";
    }

    @Override
    public String getSqlQueryColumnFields(List<String> column, String table) {
        return "SELECT " + quoteColumns(column) + " FROM " + quoteTable(table) + " FETCH FIRST  1 ROWS ONLY";
    }

    @Override
    public String getSplitFilter(String columnName) {
        return String.format("mod(%s,${N}) = ${M}", getStartQuote() + columnName + getEndQuote());
    }

    @Override
    public String getSplitFilterWithTmpTable(String tmpTable, String columnName){
        return String.format("mod(%s.%s,${N}) = ${M}", tmpTable, getStartQuote() + columnName + getEndQuote());
    }

    @Override
    public EDatabaseType getDatabaseType() {
        return EDatabaseType.DB2;
    }

    @Override
    public String getDriverClass() {
        return "com.ibm.db2.jcc.DB2Driver";
    }

    @Override
    public String getStartQuote() {
        return "";
    }

    @Override
    public String getEndQuote() {
        return "";
    }

    @Override
    public String quoteValue(String value, String column) {
        return String.format("'%s' as %s",value,column);
    }
}
