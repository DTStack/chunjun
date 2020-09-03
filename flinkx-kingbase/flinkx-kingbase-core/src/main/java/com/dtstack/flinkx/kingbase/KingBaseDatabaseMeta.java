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

package com.dtstack.flinkx.kingbase;

import com.dtstack.flinkx.enums.EDatabaseType;
import com.dtstack.flinkx.rdb.BaseDatabaseMeta;

import java.util.List;

/**
 * The class of KingBase database prototype
 *
 * Company: www.dtstack.com
 * @author kunni@dtstack.com
 */

public class KingBaseDatabaseMeta extends BaseDatabaseMeta {

    @Override
    protected String makeValues(List<String> column) {
        throw new UnsupportedOperationException();
    }

    @Override
    public EDatabaseType getDatabaseType() {
        return EDatabaseType.KingBase;
    }

    @Override
    public String getDriverClass() {
        return "com.kingbase8.Driver";
    }

    @Override
    public String getSqlQueryFields(String tableName) {
        return "SELECT * FROM " + tableName + " LIMIT 0";
    }

    @Override
    public String getSqlQueryColumnFields(List<String> column, String table) {
        return "SELECT " + quoteColumns(column) + " FROM " + quoteTable(table) + " LIMIT 0";
    }


    @Override
    public String quoteValue(String value, String column) {
        return String.format("\"%s\" as %s",value,column);
    }

    @Override
    public String getSplitFilter(String columnName) {
        return String.format("%s mod ${N} = ${M}", getStartQuote() + columnName + getEndQuote());
    }

    @Override
    public String getSplitFilterWithTmpTable(String tmpTable, String columnName) {
        return String.format("%s.%s mod ${N} = ${M}", tmpTable, getStartQuote() + columnName + getEndQuote());
    }

    @Override
    public String getStartQuote() {
        return "";
    }

    @Override
    public String getEndQuote() {
        return "";
    }
}
