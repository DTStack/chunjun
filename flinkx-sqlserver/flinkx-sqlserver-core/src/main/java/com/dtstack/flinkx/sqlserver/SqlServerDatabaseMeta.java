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

package com.dtstack.flinkx.sqlserver;

import com.dtstack.flinkx.constants.ConstantValue;
import com.dtstack.flinkx.enums.EDatabaseType;
import com.dtstack.flinkx.rdb.BaseDatabaseMeta;
import com.dtstack.flinkx.util.StringUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


/**
 * The class of SQLServer database prototype
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class SqlServerDatabaseMeta extends BaseDatabaseMeta {

    private static final long serialVersionUID = 1L;

    @Override
    public EDatabaseType getDatabaseType() {
        return EDatabaseType.SQLServer;
    }

    @Override
    public String getDriverClass() {
        return "net.sourceforge.jtds.jdbc.Driver";
    }

    @Override
    public String getSqlQueryFields(String tableName) {
        return "SELECT TOP 1 * FROM " + tableName;
    }

    @Override
    public String getSqlQueryColumnFields(List<String> column, String table) {
        return "SELECT TOP 1 " + quoteColumns(column) + " FROM " + quoteTable(table);
    }

    @Override
    public String quoteValue(String value, String column) {
        return String.format("'%s' as %s",value,column);
    }

    @Override
    public String getSplitFilter(String columnName) {
        return String.format("%s %% ${N} = ${M}", getStartQuote() + columnName + getEndQuote());
    }


    @Override
    public String quoteTable(String table) {
        List<String> strings = StringUtil.splitIgnoreQuota(table, ConstantValue.POINT_SYMBOL.charAt(0));
        return strings.stream().map(i -> {
            StringBuffer stringBuffer = new StringBuffer(64);
            return stringBuffer.append("\"").append(i).append("\"").toString();
        }).collect(Collectors.joining(ConstantValue.POINT_SYMBOL));

    }

    @Override
    public String getSplitFilterWithTmpTable(String tmpTable, String columnName) {
        return String.format("%s.%s %% ${N} = ${M}", tmpTable, getStartQuote() + columnName + getEndQuote());
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
        return sb.toString();
    }

    @Override
    public String getUpsertStatement(List<String> column, String table, Map<String,List<String>> updateKey) {
        if(updateKey == null || updateKey.isEmpty()) {
            return getInsertStatement(column, table);
        }

        List<String> updateColumns = getUpdateColumns(column, updateKey);
        if(CollectionUtils.isEmpty(updateColumns)){
            return "MERGE INTO " + quoteTable(table) + " T1 USING "
                    + "(" + makeValues(column) + ") T2 ON ("
                    + updateKeySql(updateKey) + ") WHEN NOT MATCHED THEN "
                    + "INSERT (" + quoteColumns(column) + ") VALUES ("
                    + quoteColumns(column, "T2") + ");";
        } else {
            return "MERGE INTO " + quoteTable(table) + " T1 USING "
                    + "(" + makeValues(column) + ") T2 ON ("
                    + updateKeySql(updateKey) + ") WHEN MATCHED THEN UPDATE SET "
                    + getSqlServerUpdateSql(updateColumns, updateKey,"T1", "T2") + " WHEN NOT MATCHED THEN "
                    + "INSERT (" + quoteColumns(column) + ") VALUES ("
                    + quoteColumns(column, "T2") + ");";
        }
    }

    @Override
    protected String makeReplaceValues(List<String> column, List<String> fullColumn){
        String replaceValues = super.makeReplaceValues(column,fullColumn);
        return "(select " + replaceValues + ")";
    }

    private String getSqlServerUpdateSql(List<String> column,Map<String,List<String>> updateKey, String leftTable, String rightTable) {
        List<String> pkCols = new ArrayList<>();
        for(Map.Entry<String,List<String>> entry : updateKey.entrySet()) {
            pkCols.addAll(entry.getValue());
        }

        String prefixLeft = StringUtils.isBlank(leftTable) ? "" : quoteTable(leftTable) + ".";
        String prefixRight = StringUtils.isBlank(rightTable) ? "" : quoteTable(rightTable) + ".";
        List<String> list = new ArrayList<>();

        boolean isPk = false;
        for(String col : column) {
            for (String pkCol : pkCols) {
                if (pkCol.equalsIgnoreCase(col)){
                    isPk = true;
                    break;
                }
            }

            if(isPk){
                isPk = false;
                continue;
            }

            list.add(prefixLeft + col + "=" + prefixRight + col);
            isPk = false;
        }
        return StringUtils.join(list, ",");
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
