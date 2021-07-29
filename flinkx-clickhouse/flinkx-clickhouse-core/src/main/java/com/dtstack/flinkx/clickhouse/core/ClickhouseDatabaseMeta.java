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
package com.dtstack.flinkx.clickhouse.core;

import com.dtstack.flinkx.enums.EDatabaseType;
import com.dtstack.flinkx.rdb.BaseDatabaseMeta;

import java.util.List;
import java.util.Map;

/**
 * Date: 2019/11/08
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public class ClickhouseDatabaseMeta extends BaseDatabaseMeta {
    @Override
    public EDatabaseType getDatabaseType() {
        return EDatabaseType.clickhouse;
    }

    @Override
    public String getDriverClass() {
        return "ru.yandex.clickhouse.ClickHouseDriver";
    }

    @Override
    public String getSqlQueryFields(String tableName) {
        return "SELECT * FROM " + tableName + " LIMIT 1";
    }

    @Override
    public String getSqlQueryColumnFields(List<String> column, String table) {
        return "SELECT " + quoteColumns(column) + " FROM " + quoteTable(table) + " LIMIT 1";
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
    public String quoteValue(String value, String column) {
        return String.format("'%s' as %s",value,column);
    }

    @Override
    public String getSplitFilter(String columnName) {
        return String.format("modulo(%s , ${N}) = ${M}", getStartQuote() + columnName + getEndQuote());
    }

    @Override
    public String getSplitFilterWithTmpTable(String tmpTable, String columnName){
        return String.format("modulo(%s.%s , ${N}) = ${M}", tmpTable, getStartQuote() + columnName + getEndQuote());
    }

    @Override
    public String getUpsertStatement(List<String> column, String table, Map<String,List<String>> updateKey) {
        throw new UnsupportedOperationException("upsert mode is not supported");
    }

    @Override
    protected String makeValues(List<String> column) {
        return null;
    }

    @Override
    public int getFetchSize() {
        return 1000;
    }

    @Override
    public int getQueryTimeout() {
        return 1000;
    }
}
