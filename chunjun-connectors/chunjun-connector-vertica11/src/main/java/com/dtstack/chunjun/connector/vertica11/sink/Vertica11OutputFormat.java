/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.dtstack.chunjun.connector.vertica11.sink;

import com.dtstack.chunjun.connector.jdbc.sink.JdbcOutputFormat;
import com.dtstack.chunjun.connector.jdbc.util.JdbcUtil;
import com.dtstack.chunjun.connector.vertica11.dialect.Vertica11Dialect;
import com.dtstack.chunjun.enums.EWriteMode;
import com.dtstack.chunjun.util.JsonUtil;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;

import java.sql.SQLException;
import java.util.List;

@Slf4j
public class Vertica11OutputFormat extends JdbcOutputFormat {

    private static final long serialVersionUID = 5927516457280937846L;

    @Override
    protected void openInternal(int taskNumber, int numTasks) {
        try {
            dbConn = getConnection();
            // By default, transaction auto-commit is turned off, and transactions are manually
            // controlled
            dbConn.setAutoCommit(jdbcConfig.isAutoCommit());
            if (!EWriteMode.INSERT.name().equalsIgnoreCase(jdbcConfig.getMode())) {
                List<String> updateKey = jdbcConfig.getUniqueKey();
                if (CollectionUtils.isEmpty(updateKey)) {
                    List<String> tableIndex =
                            JdbcUtil.getTablePrimaryKey(
                                    jdbcDialect.getTableIdentify(
                                            jdbcConfig.getSchema(), jdbcConfig.getTable()),
                                    dbConn);
                    jdbcConfig.setUniqueKey(tableIndex);
                    log.info("updateKey = {}", JsonUtil.toJson(tableIndex));
                }
            }

            buildStatementWrapper();
            log.info("subTask[{}}] wait finished", taskNumber);
        } catch (SQLException sqe) {
            throw new IllegalArgumentException("open() failed.", sqe);
        } finally {
            JdbcUtil.commit(dbConn);
        }
    }

    @Override
    protected String getUpsertStatement() {
        return ((Vertica11Dialect) jdbcDialect)
                .getUpsertStatement(
                        jdbcConfig.getSchema(),
                        jdbcConfig.getTable(),
                        columnNameList.toArray(new String[0]),
                        columnTypeList.toArray(new String[0]),
                        jdbcConfig.getUniqueKey().toArray(new String[0]),
                        jdbcConfig.isAllReplace())
                .get();
    }
}
