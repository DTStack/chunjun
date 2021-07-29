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
package com.dtstack.flinkx.clickhouse.format;

import com.dtstack.flinkx.clickhouse.core.ClickhouseUtil;
import com.dtstack.flinkx.rdb.outputformat.JdbcOutputFormat;
import com.dtstack.flinkx.rdb.util.DbUtil;
import com.dtstack.flinkx.util.ClassUtil;
import com.dtstack.flinkx.util.ExceptionUtil;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Date: 2019/11/05
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public class ClickhouseOutputFormat extends JdbcOutputFormat {

    private static final Logger LOG = LoggerFactory.getLogger(ClickhouseOutputFormat.class);

    @Override
    protected void openInternal(int taskNumber, int numTasks) {
        try {
            ClassUtil.forName(driverName, getClass().getClassLoader());
            dbConn = ClickhouseUtil.getConnection(dbUrl, username, password);

            if (restoreConfig.isRestore()) {
                dbConn.setAutoCommit(false);
            }

            if(CollectionUtils.isEmpty(fullColumn) || fullColumnType == null){
                initFullColumnAndType();
            }

            for (String col : column) {
                for (int i = 0; i < fullColumn.size(); i++) {
                    if (col.equalsIgnoreCase(fullColumn.get(i))) {
                        columnType.add(fullColumnType.get(i));
                        break;
                    }
                }
            }

            preparedStatement = prepareTemplates();
            readyCheckpoint = false;

            LOG.info("subTask[{}}] wait finished", taskNumber);
        } catch (SQLException sqe) {
            throw new IllegalArgumentException("open() failed.", sqe);
        }
    }

    private void initFullColumnAndType() throws SQLException {
        List<String> nameList = new ArrayList<>();
        List<String> typeList = new ArrayList<>();

        Statement stmt = null;
        ResultSet rs = null;
        try {
            stmt = dbConn.createStatement();
            rs = stmt.executeQuery("desc " + table);
            while (rs.next()) {
                nameList.add(rs.getString(1));
                typeList.add(rs.getString(2));
            }
        } catch (SQLException e) {
            LOG.error("error to get {} schema, e = {}", table, ExceptionUtil.getErrorMessage(e));
            throw e;
        }finally {
            DbUtil.closeDbResources(rs, stmt,null, false);
        }

        if(CollectionUtils.isEmpty(fullColumn)) {
            fullColumn = nameList;
        }
        if(fullColumnType == null) {
            fullColumnType = typeList;
        }
    }
}
