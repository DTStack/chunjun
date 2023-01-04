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

package com.dtstack.chunjun.connector.sqlserver.source;

import com.dtstack.chunjun.connector.jdbc.source.JdbcInputFormat;
import com.dtstack.chunjun.connector.jdbc.util.JdbcUtil;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;
import com.dtstack.chunjun.util.ExceptionUtil;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

@Slf4j
public class SqlserverInputFormat extends JdbcInputFormat {

    private static final long serialVersionUID = 8149670373029375238L;

    @Override
    public boolean reachedEnd() {
        if (hasNext) {
            return false;
        } else {
            if (jdbcConfig.isPolling()) {
                try {
                    TimeUnit.MILLISECONDS.sleep(jdbcConfig.getPollingInterval());
                    // 间隔轮询检测数据库连接是否断开，超时时间三秒，断开后自动重连
                    if (checkConnectionIsValid(dbConn)) {
                        dbConn = getConnection();
                        // 重新连接后还是不可用则认为数据库异常，任务失败
                        if (checkConnectionIsValid(dbConn)) {
                            String message =
                                    String.format(
                                            "cannot connect to %s, username = %s, please check %s is available.",
                                            jdbcConfig.getJdbcUrl(),
                                            jdbcConfig.getUsername(),
                                            jdbcDialect.dialectName());
                            throw new ChunJunRuntimeException(message);
                        }
                    }
                    if (!dbConn.getAutoCommit()) {
                        dbConn.setAutoCommit(true);
                    }
                    JdbcUtil.closeDbResources(resultSet, null, null, false);
                    // 此处endLocation理应不会为空
                    queryForPolling(incrementKeyUtil.transToLocationValue(state).toString());
                    return false;
                } catch (InterruptedException e) {
                    log.warn("interrupted while waiting for polling", e);
                } catch (SQLException e) {
                    JdbcUtil.closeDbResources(resultSet, ps, null, false);
                    String message =
                            String.format(
                                    "error to execute sql = %s, startLocation = %s, e = %s",
                                    jdbcConfig.getQuerySql(),
                                    state,
                                    ExceptionUtil.getErrorMessage(e));
                    throw new ChunJunRuntimeException(message, e);
                }
            }
            return true;
        }
    }

    /**
     * Returns true if the connection has not been closed and is still valid.
     *
     * @param connection jdbc connection
     */
    public boolean checkConnectionIsValid(Connection connection) {
        try {
            if (connection.isClosed()) {
                return true;
            }
            final String validationQuery = "select 1";
            try (Statement statement = connection.createStatement();
                    ResultSet resultSet = statement.executeQuery(validationQuery)) {
                if (!resultSet.next()) {
                    return true;
                }
            }
        } catch (Throwable e) {
            return true;
        }
        return false;
    }
}
