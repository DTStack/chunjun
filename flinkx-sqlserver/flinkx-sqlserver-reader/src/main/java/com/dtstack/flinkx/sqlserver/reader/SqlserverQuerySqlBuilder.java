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
package com.dtstack.flinkx.sqlserver.reader;

import com.dtstack.flinkx.rdb.datareader.JdbcDataReader;
import com.dtstack.flinkx.rdb.datareader.QuerySqlBuilder;
import com.dtstack.flinkx.rdb.util.DbUtil;
import com.dtstack.flinkx.sqlserver.SqlServerConstants;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

/**
 * SqlserverQuerySqlBuilder
 *
 * @author by dujie@dtstack.com
 * @Date 2020/8/18
 */
public class SqlserverQuerySqlBuilder extends QuerySqlBuilder {
    //是否在sql语句后面添加 with(nolock) ,默认是false
    private Boolean withNoLock = false;

    public SqlserverQuerySqlBuilder(JdbcDataReader reader) {
        super(reader);
        if (reader instanceof SqlserverReader) {
            this.withNoLock = ((SqlserverReader) reader).getWithNoLock();
        }
    }

    @Override
    protected String buildQuerySql() {
        List<String> selectColumns = DbUtil.buildSelectColumns(databaseInterface, metaColumns);
        boolean splitWithRowNum = addRowNumColumn(databaseInterface, selectColumns, isSplitByKey, splitKey);

        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ").append(StringUtils.join(selectColumns, ",")).append(" FROM ");
        sb.append(databaseInterface.quoteTable(table));
        //是否需要添加 with(nolock)，添加规则是 from table with(nolock)
        if (withNoLock) {
            sb.append(SqlServerConstants.WITH_NO_LOCK);
        }
        sb.append(" WHERE 1=1 ");

        StringBuilder filter = new StringBuilder();

        if (isSplitByKey && !splitWithRowNum) {
            filter.append(" AND ").append(databaseInterface.getSplitFilter(splitKey));
        }

        if (customFilter != null) {
            customFilter = customFilter.trim();
            if (customFilter.length() > 0) {
                filter.append(" AND ").append(customFilter);
            }
        }

        if (isIncrement) {
            filter.append(" ").append(INCREMENT_FILTER_PLACEHOLDER);
        }

        if (isRestore) {
            filter.append(" ").append(RESTORE_FILTER_PLACEHOLDER);
        }

        sb.append(filter);

        if (isSplitByKey && splitWithRowNum) {
            return String.format(SQL_SPLIT_WITH_ROW_NUM, sb.toString(), databaseInterface.getSplitFilter(ROW_NUM_COLUMN_ALIAS));
        } else {
            return sb.toString();
        }
    }

    @Override
    protected String buildQuerySqlWithCustomSql() {
        StringBuilder querySql = new StringBuilder();
        querySql.append(String.format(CUSTOM_SQL_TEMPLATE, customSql, TEMPORARY_TABLE_NAME));
        //是否需要添加 with(nolock)，添加规则是 from table with(nolock)
        if (withNoLock) {
            querySql.append(SqlServerConstants.WITH_NO_LOCK);
        }
        querySql.append(" WHERE 1=1 ");

        if (isSplitByKey) {
            querySql.append(" And ").append(databaseInterface.getSplitFilterWithTmpTable(TEMPORARY_TABLE_NAME, splitKey));
        }

        if (isIncrement) {
            querySql.append(" ").append(INCREMENT_FILTER_PLACEHOLDER);
        }

        if (isRestore) {
            querySql.append(" ").append(RESTORE_FILTER_PLACEHOLDER);
        }

        if (customFilter != null) {
            customFilter = customFilter.trim();
            if (customFilter.length() > 0) {
                querySql.append(" AND ").append(customFilter);
            }
        }

        return querySql.toString();
    }
}
