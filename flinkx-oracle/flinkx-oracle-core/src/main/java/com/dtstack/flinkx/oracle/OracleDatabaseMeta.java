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
import java.util.List;

/**
 * The class of Oracle database prototype
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class OracleDatabaseMeta extends BaseDatabaseMeta {
    @Override
    public String quoteTable(String table) {
        String[] part = table.split("\\.");
        if(part.length == 2) {
            table = part[0] + "." + getStartQuote() + part[1] + getEndQuote();
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
    public String getSplitFilter(String columnName) {
        return String.format("mod(%s, ${N}) = ${M}", getStartQuote() + columnName + getEndQuote());
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
    public int getFetchSize(){
        return 1000;
    }

    @Override
    public int getQueryTimeout(){
        return 1000;
    }
}
