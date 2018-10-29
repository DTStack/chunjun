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

import com.dtstack.flinkx.rdb.BaseDatabaseMeta;
import org.apache.commons.lang.StringUtils;
import java.util.List;


/**
 * The class of SQLServer database prototype
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class SqlServerDatabaseMeta extends BaseDatabaseMeta {
    @Override
    public String getDatabaseType() {
        return "sqlserver";
    }

    @Override
    public String getDriverClass() {
        return "com.microsoft.sqlserver.jdbc.SQLServerDriver";
    }

    @Override
    public String getSQLQueryFields(String tableName) {
        return "SELECT TOP 1 * FROM " + tableName;
    }

    @Override
    public String getSQLQueryColumnFields(List<String> column, String table) {
        return "SELECT TOP 1 " + quoteColumns(column) + " FROM " + quoteTable(table);
    }

    @Override
    public String getSplitFilter(String columnName) {
        return String.format("%s %% ${N} = ${M}", getStartQuote() + columnName + getEndQuote());
    }

    @Override
    protected String makeMultipleValues(int nCols, int batchSize) {
        String value = makeValues(nCols);
        return StringUtils.repeat(value, " UNION ALL ", batchSize);
    }

    @Override
    protected String makeValues(int nCols) {
        return "SELECT " + StringUtils.repeat("?", ",", nCols);
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
    public int getFetchSize(){
        return 1000;
    }

    @Override
    public int getQueryTimeout(){
        return 1000;
    }
}
