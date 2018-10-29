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

package com.dtstack.flinkx.mysql;

import com.dtstack.flinkx.rdb.BaseDatabaseMeta;
import org.apache.commons.lang3.StringUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The class of MySQL database prototype
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class MySqlDatabaseMeta extends BaseDatabaseMeta {

    @Override
    public String getDatabaseType() {
        return "mysql";
    }

    @Override
    public String getDriverClass() {
        return "com.mysql.jdbc.Driver";
    }

    @Override
    public String getSQLQueryFields(String tableName) {
        return "SELECT * FROM " + tableName + " LIMIT 0";
    }

    @Override
    public String getSQLQueryColumnFields(List<String> column, String table) {
        return "SELECT " + quoteColumns(column) + " FROM " + quoteTable(table) + " LIMIT 0";
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
    public String getReplaceStatement(List<String> column, List<String> fullColumn, String table, Map<String,List<String>> updateKey) {
        return "REPLACE INTO " + quoteTable(table)
                + " (" + quoteColumns(column) + ") values "
                + makeValues(column.size());
    }

    @Override
    public String getUpsertStatement(List<String> column, String table, Map<String,List<String>> updateKey) {
        return "INSERT INTO " + quoteTable(table)
                + " (" + quoteColumns(column) + ") values "
                + makeValues(column.size())
                + " ON DUPLICATE KEY UPDATE "
                + makeUpdatePart(column);
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
    public String getMultiReplaceStatement(List<String> column, List<String> fullColumn, String table, int batchSize, Map<String,List<String>> updateKey) {
        return "REPLACE INTO " + quoteTable(table)
                + " (" + quoteColumns(column) + ") values "
                + makeMultipleValues(column.size(), batchSize);
    }

    @Override
    public String getMultiUpsertStatement(List<String> column, String table, int batchSize, Map<String,List<String>> updateKey) {
        return "INSERT INTO " + quoteTable(table)
                + " (" + quoteColumns(column) + ") values "
                + makeMultipleValues(column.size(), batchSize)
                + " ON DUPLICATE KEY UPDATE "
                + makeUpdatePart(column);
    }

    @Override
    public String getSplitFilter(String columnName) {
        return String.format("%s mod ${N} = ${M}", getStartQuote() + columnName + getEndQuote());
    }

    @Override
    public String getMultiInsertStatement(List<String> column, String table, int batchSize) {
        return "INSERT INTO " + quoteTable(table)
                + " (" + quoteColumns(column) + ") values "
                + makeMultipleValues(column.size(), batchSize);
    }

    @Override
    protected String makeValues(int nCols) {
        return "(" + StringUtils.repeat("?", ",", nCols) + ")";
    }

    @Override
    protected String makeValues(List<String> column) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected String makeMultipleValues(int nCols, int batchSize) {
        String value = makeValues(nCols);
        return StringUtils.repeat(value, ",", batchSize);
    }

    @Override
    protected String makeMultipleValues(List<String> column, int batchSize) {
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
