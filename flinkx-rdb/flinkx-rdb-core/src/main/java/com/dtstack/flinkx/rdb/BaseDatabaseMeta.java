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

package com.dtstack.flinkx.rdb;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import java.io.Serializable;
import java.util.*;

/**
 * Abstract base parent class of other database prototype implementations
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public abstract class BaseDatabaseMeta implements DatabaseInterface, Serializable {

    @Override
    public String getStartQuote() {
        return "\"";
    }

    @Override
    public String getEndQuote() {
        return "\"";
    }

    @Override
    public String quoteColumn(String column) {
        return getStartQuote() + column + getEndQuote();
    }

    @Override
    public String quoteColumns(List<String> column) {
        return quoteColumns(column, null);
    }

    @Override
    public String quoteColumns(List<String> column, String table) {
        String prefix = StringUtils.isBlank(table) ? "" : quoteTable(table) + ".";
        List<String> list = new ArrayList<>();
        for(String col : column) {
            list.add(prefix + quoteColumn(col));
        }
        return StringUtils.join(list, ",");
    }

    @Override
    public String quoteTable(String table) {
        String[] parts = table.split("\\.");
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < parts.length; ++i) {
            if(i != 0) {
                sb.append(".");
            }
            sb.append(getStartQuote() + parts[i] + getEndQuote());
        }
        return sb.toString();
    }

    @Override
    public String getReplaceStatement(List<String> column, List<String> fullColumn, String table, Map<String,List<String>> updateKey) {
        throw new UnsupportedOperationException("replace mode is not supported");
    }

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

        return StringUtils.join(values,",");
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
                    + quoteColumns(column, "T2") + ")";
        } else {
            return "MERGE INTO " + quoteTable(table) + " T1 USING "
                    + "(" + makeValues(column) + ") T2 ON ("
                    + updateKeySql(updateKey) + ") WHEN MATCHED THEN UPDATE SET "
                    + getUpdateSql(updateColumns, "T1", "T2") + " WHEN NOT MATCHED THEN "
                    + "INSERT (" + quoteColumns(column) + ") VALUES ("
                    + quoteColumns(column, "T2") + ")";
        }
    }

    /**
     * 获取都需要更新数据的字段
     */
    protected List<String> getUpdateColumns(List<String> column, Map<String,List<String>> updateKey){
        Set<String> indexColumns = new HashSet<>();
        for (List<String> value : updateKey.values()) {
            indexColumns.addAll(value);
        }

        List<String> updateColumns = new ArrayList<>();
        for (String col : column) {
            if(!indexColumns.contains(col)){
                updateColumns.add(col);
            }
        }

        return updateColumns;
    }

    /**
     * 构造查询sql
     *
     * @param column 字段列表
     * @return 查询sql
     */
    abstract protected String makeValues(List<String> column);

    protected String getUpdateSql(List<String> column, String leftTable, String rightTable) {
        String prefixLeft = StringUtils.isBlank(leftTable) ? "" : quoteTable(leftTable) + ".";
        String prefixRight = StringUtils.isBlank(rightTable) ? "" : quoteTable(rightTable) + ".";
        List<String> list = new ArrayList<>();
        for(String col : column) {
            list.add(prefixLeft + col + "=" + prefixRight + col);
        }
        return StringUtils.join(list, ",");
    }

    protected String updateKeySql(Map<String,List<String>> updateKey) {
        List<String> exprList = new ArrayList<>();
        for(Map.Entry<String,List<String>> entry : updateKey.entrySet()) {
            List<String> colList = new ArrayList<>();
            for(String col : entry.getValue()) {
                colList.add("T1." + quoteColumn(col) + "=T2." + quoteColumn(col));
            }
            exprList.add(StringUtils.join(colList, " AND "));
        }
        return StringUtils.join(exprList, " OR ");
    }

    @Override
    public String getInsertStatement(List<String> column, String table) {
        return "INSERT INTO " + quoteTable(table)
                + " (" + quoteColumns(column) + ") values ("
                + StringUtils.repeat("?", ",", column.size()) + ")";
    }

    @Override
    public String getRowNumColumn(String orderBy) {
        return String.format("row_number() over(%s) as FLINKX_ROWNUM", orderBy);
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
