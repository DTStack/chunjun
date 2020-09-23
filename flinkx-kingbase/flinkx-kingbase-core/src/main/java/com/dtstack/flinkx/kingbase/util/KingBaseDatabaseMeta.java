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

package com.dtstack.flinkx.kingbase.util;

import com.dtstack.flinkx.enums.EDatabaseType;
import com.dtstack.flinkx.rdb.BaseDatabaseMeta;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.dtstack.flinkx.constants.ConstantValue.COMMA_SYMBOL;
import static com.dtstack.flinkx.constants.ConstantValue.LEFT_PARENTHESIS_SYMBOL;
import static com.dtstack.flinkx.kingbase.constants.KingbaseCons.DRIVER;
import static com.dtstack.flinkx.kingbase.constants.KingbaseCons.KEY_UPDATE_KEY;

/**
 * The class of KingBase database prototype
 *
 * Company: www.dtstack.com
 * @author kunni@dtstack.com
 */

public class KingBaseDatabaseMeta extends BaseDatabaseMeta {

    @Override
    protected String makeValues(List<String> column) {
        StringBuilder sb = new StringBuilder();
        sb.append(LEFT_PARENTHESIS_SYMBOL);
        for(int i = 0; i < column.size(); ++i) {
            if(i != 0) {
                sb.append(COMMA_SYMBOL);
            }
            sb.append(quoteColumn(column.get(i)));
        }
        sb.append(")");
        return sb.toString();
    }

    @Override
    public EDatabaseType getDatabaseType() {
        return EDatabaseType.KingBase;
    }

    @Override
    public String getDriverClass() {
        return DRIVER;
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
    public String getUpsertStatement(List<String> column, String table, Map<String,List<String>> updateKey) {
        return "INSERT INTO " + quoteTable(table)
                + " (" + quoteColumns(column) + ") VALUES "
                + makeValues(column.size())
                + " ON CONFLICT " +makeValues(updateKey.get(KEY_UPDATE_KEY)) + " DO UPDATE SET "
                + makeUpdatePart(column);
    }

    private String makeValues(int nCols) {
        return "(" + StringUtils.repeat("?", ",", nCols) + ")";
    }

    private String makeUpdatePart (List<String> column) {
        List<String> updateList = new ArrayList<>();
        for(String col : column) {
            String quotedCol = quoteColumn(col);
            updateList.add(quotedCol + "=EXCLUDED." + quotedCol);
        }
        return StringUtils.join(updateList, COMMA_SYMBOL);
    }

    @Override
    public String quoteValue(String value, String column) {
        return String.format("\"%s\" as %s",value,column);
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
    public String getStartQuote() {
        return "";
    }

    @Override
    public String getEndQuote() {
        return "";
    }
}
