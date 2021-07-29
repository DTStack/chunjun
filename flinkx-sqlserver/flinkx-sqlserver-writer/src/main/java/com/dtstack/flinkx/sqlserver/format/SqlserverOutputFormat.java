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
package com.dtstack.flinkx.sqlserver.format;

import com.dtstack.flinkx.rdb.outputformat.JdbcOutputFormat;
import com.dtstack.flinkx.rdb.util.DbUtil;
import com.dtstack.flinkx.sqlserver.SqlServerConstants;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Date: 2019/09/20
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public class SqlserverOutputFormat extends JdbcOutputFormat {
    private static final Logger LOG = LoggerFactory.getLogger(SqlserverOutputFormat.class);

    //是否在sql语句后面添加 with(nolock) ,默认是false
    private Boolean withNoLock;

    @Override
    protected void beforeWriteRecords() {
        if (CollectionUtils.isNotEmpty(preSql)) {
            super.beforeWriteRecords();
        }
        Statement stmt = null;
        String sql = String.format("IF OBJECTPROPERTY(OBJECT_ID('%s'),'TableHasIdentity')=1 BEGIN SET IDENTITY_INSERT \"%s\" ON  END", table, table);
        try {
            stmt = dbConn.createStatement();
            stmt.execute(sql);
        } catch (SQLException e) {
            LOG.error("error to execute {}", sql);
            throw new RuntimeException(e);
        } finally {
            DbUtil.closeDbResources(null, stmt, null, false);
        }
    }

    @Override
    protected boolean needWaitBeforeWriteRecords() {
        return true;
    }


    @Override
    protected List<String> analyzeTable() {
        List<String> ret = new ArrayList<>();
        Statement stmt = null;
        ResultSet rs = null;
        try {
            stmt = dbConn.createStatement();
            String queryFieldsSql = databaseInterface.getSqlQueryFields(databaseInterface.quoteTable(table));
            //是否需要添加 with(nolock)，添加规则是 from table with(nolock)
            if (getWithNoLock()) {
                //databaseInterface.getSqlQueryFields 返回的结果就是from table  后面没有where等语句所以直接添加的
                queryFieldsSql += SqlServerConstants.WITH_NO_LOCK;
            }
            rs = stmt.executeQuery(queryFieldsSql);
            ResultSetMetaData rd = rs.getMetaData();
            for (int i = 0; i < rd.getColumnCount(); ++i) {
                ret.add(rd.getColumnTypeName(i + 1));
            }

            if (CollectionUtils.isEmpty(fullColumn)) {
                for (int i = 0; i < rd.getColumnCount(); ++i) {
                    fullColumn.add(rd.getColumnName(i + 1));
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            DbUtil.closeDbResources(rs, stmt, null, false);
        }

        return ret;
    }

    public Boolean getWithNoLock() {
        return withNoLock;
    }

    public void setWithNoLock(Boolean withNoLock) {
        this.withNoLock = withNoLock;
    }
}
