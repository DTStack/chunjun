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

package com.dtstack.flinkx.oracle;

import com.dtstack.flinkx.enums.EDatabaseType;
import com.dtstack.flinkx.rdb.BaseDatabaseMeta;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The class of Oracle database prototype
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class OracleDatabaseMeta extends BaseDatabaseMeta {

    @Override
    public String quoteTable(String table) {
        table = table.replace("\"","");
        String[] part = table.split("\\.");
        if(part.length == 2) {
            table = part[0] + "." + getStartQuote() + part[1] + getEndQuote();
        } else {
            table = getStartQuote() + table + getEndQuote();
        }
        return table;
    }

    @Override
    public EDatabaseType getDatabaseType() {
        return EDatabaseType.Oracle;
    }

    @Override
    public String getDriverClass() {
        return "oracle.jdbc.driver.OracleDriver";
    }

    @Override
    public String getSQLQueryFields(String tableName) {
        return "SELECT /*+FIRST_ROWS*/ * FROM " + tableName + " WHERE ROWNUM < 1";
    }

    @Override
    public String getSQLQueryColumnFields(List<String> column, String table) {
        return "SELECT /*+FIRST_ROWS*/ " + quoteColumns(column) + " FROM " + quoteTable(table) + " WHERE ROWNUM < 1";
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
    protected String makeMultipleValues(int nCols, int batchSize) {
        String value = makeValues(nCols);
        return StringUtils.repeat(value, " UNION ALL ", batchSize);
    }

    @Override
    public String getMultiInsertStatement(List<String> column, String table, int batchSize) {
        return "INSERT INTO " + quoteTable(table)
                + " (" + quoteColumns(column) + ") "
                + makeMultipleValues(column.size(), batchSize);
    }

    @Override
    protected String makeValues(int nCols) {
        return "SELECT " + StringUtils.repeat("?", ",", nCols) + " FROM DUAL";
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
        sb.append(" FROM DUAL");
        return sb.toString();
    }

    @Override
    protected String makeReplaceValues(List<String> column, List<String> fullColumn){
        List<String> values = new ArrayList<>();
        boolean contains = false;

        for (String col : column) {
            values.add("? " + quoteColumn(col));
        }

        for (String col : fullColumn) {
            for (String c : column) {
                if (c.equalsIgnoreCase(col)){
                    contains = true;
                    break;
                }
            }

            if (contains){
                contains = false;
                continue;
            } else {
                values.add("null "  + quoteColumn(col));
            }

            contains = false;
        }

        return "SELECT " + org.apache.commons.lang3.StringUtils.join(values,",") + " FROM DUAL";
    }

    @Override
    public String getReplaceStatement(List<String> column, List<String> fullColumn, String table, Map<String,List<String>> updateKey) {
        return getMultiReplaceStatement(column,fullColumn,table,1,updateKey);
    }

    @Override
    public String getMultiReplaceStatement(List<String> column, List<String> fullColumn, String table, int batchSize, Map<String,List<String>> updateKey) {
        if(updateKey == null || updateKey.isEmpty()) {
            return getMultiInsertStatement(column, table, batchSize);
        }

        return "MERGE INTO " + quoteTable(table) + " T1 USING "
                + "(" + makeMultipleReplaceValues(column,fullColumn,batchSize) + ") T2 ON ("
                + updateKeySql(updateKey) + ") WHEN MATCHED THEN UPDATE SET "
                + getUpdateSql(fullColumn, updateKey,"T1", "T2") + " WHEN NOT MATCHED THEN "
                + "INSERT (" + quoteColumns(fullColumn) + ") VALUES ("
                + quoteColumns(fullColumn, "T2") + ")";
    }

    @Override
    public String getUpsertStatement(List<String> column, String table, Map<String,List<String>> updateKey) {
        return getMultiUpsertStatement(column,table,1,updateKey);
    }

    @Override
    public String getMultiUpsertStatement(List<String> column, String table, int batchSize, Map<String,List<String>> updateKey) {
        if(updateKey == null || updateKey.isEmpty()) {
            return getMultiInsertStatement(column, table, batchSize);
        }

        return "MERGE INTO " + quoteTable(table) + " T1 USING "
                + "(" + makeMultipleValues(column,batchSize) + ") T2 ON ("
                + updateKeySql(updateKey) + ") WHEN MATCHED THEN UPDATE SET "
                + getUpdateSql(column, updateKey,"T1", "T2") + " WHEN NOT MATCHED THEN "
                + "INSERT (" + quoteColumns(column) + ") VALUES ("
                + quoteColumns(column, "T2") + ")";
    }

    private String getUpdateSql(List<String> column,Map<String,List<String>> updateKey, String leftTable, String rightTable) {
        String prefixLeft = StringUtils.isBlank(leftTable) ? "" : quoteTable(leftTable) + ".";
        String prefixRight = StringUtils.isBlank(rightTable) ? "" : quoteTable(rightTable) + ".";
        List<String> list = new ArrayList<>();

        List<String> pkCols = new ArrayList<>();
        for (List<String> value : updateKey.values()) {
            pkCols.addAll(value);
        }

        for(String col : column) {
            if (pkCols.contains(col)){
                continue;
            }

            list.add(prefixLeft + quoteColumn(col) + "=" + prefixRight + quoteColumn(col));
        }
        return StringUtils.join(list, ",");
    }

    @Override
    public int getFetchSize(){
        return 1000;
    }

    @Override
    public int getQueryTimeout(){
        return 3000;
    }
}
