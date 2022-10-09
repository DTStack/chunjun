/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.dtstack.chunjun.connector.vertica11.sink;

import com.dtstack.chunjun.connector.jdbc.sink.JdbcOutputFormat;
import com.dtstack.chunjun.connector.jdbc.util.JdbcUtil;
import com.dtstack.chunjun.connector.vertica11.dialect.Vertica11Dialect;
import com.dtstack.chunjun.enums.EWriteMode;
import com.dtstack.chunjun.enums.Semantic;
import com.dtstack.chunjun.util.JsonUtil;

import org.apache.commons.collections.CollectionUtils;

import java.sql.SQLException;
import java.util.List;

/** @author menghan on 2022/7/12. */
public class Vertica11OutputFormat extends JdbcOutputFormat {
    @Override
    protected void openInternal(int taskNumber, int numTasks) {
        try {
            dbConn = getConnection();
            // By default, transaction auto-commit is turned off, and transactions are manually
            // controlled
            if (Semantic.EXACTLY_ONCE == semantic) {
                dbConn.setAutoCommit(false);
            }
            if (!EWriteMode.INSERT.name().equalsIgnoreCase(jdbcConf.getMode())) {
                List<String> updateKey = jdbcConf.getUniqueKey();
                if (CollectionUtils.isEmpty(updateKey)) {
                    List<String> tableIndex =
                            JdbcUtil.getTablePrimaryKey(
                                    jdbcConf.getSchema(), jdbcConf.getTable(), dbConn);
                    jdbcConf.setUniqueKey(tableIndex);
                    LOG.info("updateKey = {}", JsonUtil.toJson(tableIndex));
                }
            }

            buildStmtProxy();
            LOG.info("subTask[{}}] wait finished", taskNumber);
        } catch (SQLException sqe) {
            throw new IllegalArgumentException("open() failed.", sqe);
        } finally {
            JdbcUtil.commit(dbConn);
        }
    }

    @Override
    protected String prepareTemplates() {
        String singleSql;
        if (EWriteMode.INSERT.name().equalsIgnoreCase(jdbcConf.getMode())) {
            singleSql =
                    jdbcDialect.getInsertIntoStatement(
                            jdbcConf.getSchema(),
                            jdbcConf.getTable(),
                            columnNameList.toArray(new String[0]));
        } else if (EWriteMode.UPDATE.name().equalsIgnoreCase(jdbcConf.getMode())) {
            singleSql =
                    ((Vertica11Dialect) jdbcDialect)
                            .getUpsertStatement(
                                    jdbcConf.getSchema(),
                                    jdbcConf.getTable(),
                                    columnNameList.toArray(new String[0]),
                                    columnTypeList.toArray(new String[0]),
                                    jdbcConf.getUniqueKey().toArray(new String[0]),
                                    jdbcConf.isAllReplace())
                            .get();
        } else {
            throw new IllegalArgumentException("Unknown write mode:" + jdbcConf.getMode());
        }
        LOG.info("write sql:{}", singleSql);
        return singleSql;
    }
}
