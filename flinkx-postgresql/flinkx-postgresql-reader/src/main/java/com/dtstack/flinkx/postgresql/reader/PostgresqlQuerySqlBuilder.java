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
package com.dtstack.flinkx.postgresql.reader;

import com.dtstack.flinkx.rdb.datareader.JdbcDataReader;
import com.dtstack.flinkx.rdb.datareader.QuerySqlBuilder;
import com.dtstack.flinkx.rdb.util.DbUtil;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

/**
 * Date: 2019/09/20
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public class PostgresqlQuerySqlBuilder extends QuerySqlBuilder {

    public PostgresqlQuerySqlBuilder(JdbcDataReader reader){
        super(reader);
    }

    @Override
    protected String buildQuerySql(){
        List<String> selectColumns = DbUtil.buildSelectColumns(databaseInterface, metaColumns);
        boolean splitWithRowNum = addRowNumColumn(databaseInterface, selectColumns, isSplitByKey, splitKey);

        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ").append(StringUtils.join(selectColumns,",")).append(" FROM ");
        sb.append(databaseInterface.quoteTable(table));
        sb.append(" WHERE 1=1 ");

        StringBuilder filter = new StringBuilder();

        if(isSplitByKey && !splitWithRowNum) {
            filter.append(" AND ").append(databaseInterface.getSplitFilter(splitKey));
        }

        if (customFilter != null){
            customFilter = customFilter.trim();
            if (customFilter.length() > 0){
                filter.append(" AND ").append(customFilter);
            }
        }

        if(isIncrement){
            filter.append(" ").append(INCREMENT_FILTER_PLACEHOLDER);
        }

        if(isRestore){
            filter.append(" ").append(RESTORE_FILTER_PLACEHOLDER);
        }

        sb.append(filter);
        sb.append(buildOrderSql());

        if(isSplitByKey && splitWithRowNum){
            return String.format(SQL_SPLIT_WITH_ROW_NUM, sb.toString(), databaseInterface.getSplitFilter(ROW_NUM_COLUMN_ALIAS));
        } else {
            return sb.toString();
        }
    }
}
