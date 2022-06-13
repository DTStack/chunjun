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
import com.dtstack.chunjun.enums.ColumnType;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;
import com.dtstack.chunjun.throwable.ReadRecordException;
import com.dtstack.chunjun.util.ExceptionUtil;
import org.apache.flink.table.data.RowData;
import org.apache.commons.lang3.StringUtils;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Company：www.dtstack.com
 *
 * @author shitou
 * @date 2021/5/19 13:57
 */
public class SqlserverInputFormat extends JdbcInputFormat {

    /**
     * 构建边界位置sql
     *
     * @param incrementColType 增量字段类型
     * @param incrementCol     增量字段名称
     * @param location         边界位置(起始/结束)
     * @param operator         判断符( >, >=, <)
     * @return
     */
    @Override
    protected String getLocationSql(
            String incrementColType, String incrementCol, String location, String operator) {
        String endTimeStr;
        String endLocationSql;
        boolean isTimeType =
                ColumnType.isTimeType(incrementColType)
                        || ColumnType.NVARCHAR.name().equals(incrementColType);
        if (isTimeType) {
            if (incrementColType.equalsIgnoreCase(ColumnType.TIMESTAMP.name())) {
                endTimeStr = location;
            } else {
                endTimeStr = getTimeStr(Long.parseLong(location));
            }
            endLocationSql = incrementCol + operator + endTimeStr;
        } else if (ColumnType.isNumberType(incrementColType)) {
            endLocationSql = incrementCol + operator + location;
        } else {
            endTimeStr = String.format("'%s'", location);
            endLocationSql = incrementCol + operator + endTimeStr;
        }

        return endLocationSql;
    }

    /**
     * 构建时间边界字符串
     *
     * @param location 边界位置(起始/结束)
     * @return
     */
    @Override
    protected String getTimeStr(Long location) {
        String timeStr;
        Timestamp ts = new Timestamp(JdbcUtil.getMillis(location));
        ts.setNanos(JdbcUtil.getNanos(location));
        timeStr = JdbcUtil.getNanosTimeStr(ts.toString());
        timeStr = timeStr.substring(0, 23);
        timeStr = String.format("'%s'", timeStr);

        return timeStr;
    }

    @Override
    public boolean reachedEnd() {
        if (hasNext) {
            return false;
        } else {
            if (jdbcConf.isPolling()) {
                try {
                    TimeUnit.MILLISECONDS.sleep(jdbcConf.getPollingInterval());
                    // 间隔轮询检测数据库连接是否断开，超时时间三秒，断开后自动重连
                    if (!isValid(dbConn, 3)) {
                        dbConn = getConnection();
                        // 重新连接后还是不可用则认为数据库异常，任务失败
                        if (!isValid(dbConn, 3)) {
                            String message =
                                    String.format(
                                            "cannot connect to %s, username = %s, please check %s is available.",
                                            jdbcConf.getJdbcUrl(),
                                            jdbcConf.getUsername(),
                                            jdbcDialect.dialectName());
                            throw new ChunJunRuntimeException(message);
                        }
                    }
                    if (!dbConn.getAutoCommit()) {
                        dbConn.setAutoCommit(true);
                    }
                    JdbcUtil.closeDbResources(resultSet, null, null, false);
                    // 此处endLocation理应不会为空
                    String location;
                    if (state instanceof byte[]) {
                        location = new String((byte[]) state);
                    } else {
                        location = String.valueOf(state);
                    }
                    queryForPolling(location);
                    return false;
                } catch (InterruptedException e) {
                    LOG.warn("interrupted while waiting for polling, e = {}", e);
                } catch (SQLException e) {
                    JdbcUtil.closeDbResources(resultSet, ps, null, false);
                    String message =
                            String.format(
                                    "error to execute sql = %s, startLocation = %s, e = %s",
                                    jdbcConf.getQuerySql(),
                                    state,
                                    ExceptionUtil.getErrorMessage(e));
                    throw new ChunJunRuntimeException(message, e);
                }
            }
            return true;
        }
    }

    /**
     * 增量轮询查询
     *
     * @param startLocation
     * @throws SQLException
     */
    protected void queryForPolling(String startLocation) throws SQLException {
        // 每隔五分钟打印一次，(当前时间 - 任务开始时间) % 300秒 <= 一个间隔轮询周期
        if ((System.currentTimeMillis() - startTime) % 300000 <= jdbcConf.getPollingInterval()) {
            LOG.info("polling startLocation = {}", startLocation);
        } else {
            LOG.debug("polling startLocation = {}", startLocation);
        }

        boolean isNumber = StringUtils.isNumeric(startLocation);
        switch (type) {
            case TIMESTAMP:
                ps.setInt(1, Integer.parseInt(startLocation));
                break;
            case DATE:
                Date date =
                        isNumber
                                ? new Date(Long.parseLong(startLocation))
                                : Date.valueOf(startLocation);
                ps.setDate(1, date);
                break;
            default:
                if (isNumber) {
                    ps.setLong(1, Long.parseLong(startLocation));
                } else {
                    ps.setString(1, startLocation);
                }
        }
        resultSet = ps.executeQuery();
        hasNext = resultSet.next();
    }

    @Override
    public RowData nextRecordInternal(RowData rowData) throws ReadRecordException {
        if (!hasNext) {
            return null;
        }
        try {
            @SuppressWarnings("unchecked")
            RowData finalRowData = rowConverter.toInternal(resultSet);
            if (needUpdateEndLocation) {
                Object obj;
                switch (type) {
                    case DATETIME:
                    case DATE:
                        obj = resultSet.getTimestamp(jdbcConf.getIncreColumn()).getTime();
                        break;
                    case TIMESTAMP:
                        obj =
                                Integer.parseInt(
                                        String.valueOf(
                                                ByteBuffer.wrap(
                                                                (byte[])
                                                                        resultSet.getObject(
                                                                                jdbcConf
                                                                                        .getRestoreColumnIndex()
                                                                                        + 1))
                                                        .getLong()));
                        break;
                    default:
                        obj = resultSet.getObject(jdbcConf.getIncreColumn());
                }
                String location = String.valueOf(obj);
                endLocationAccumulator.add(new BigInteger(location));
                LOG.debug("update endLocationAccumulator, current Location = {}", location);
            }
            if (jdbcConf.getRestoreColumnIndex() > -1) {
                state = resultSet.getObject(jdbcConf.getRestoreColumnIndex() + 1);
                if (state instanceof byte[]) {
                    state =
                            Integer.parseInt(
                                    String.valueOf(
                                            ByteBuffer.wrap(
                                                            (byte[])
                                                                    resultSet.getObject(
                                                                            jdbcConf
                                                                                    .getRestoreColumnIndex()
                                                                                    + 1))
                                                    .getLong()));
                }
            }
            return finalRowData;
        } catch (Exception se) {
            throw new ReadRecordException("", se, 0, rowData);
        } finally {
            try {
                hasNext = resultSet.next();
            } catch (SQLException e) {
                LOG.error("can not read next record", e);
                hasNext = false;
            }
        }
    }

    /**
     * Returns true if the connection has not been closed and is still valid.
     *
     * @param connection jdbc connection
     * @param timeOut    The time in seconds to wait for the database operation.
     */
    public boolean isValid(Connection connection, int timeOut) {
        try {
            if (connection.isClosed()) {
                return false;
            }
            Statement statement = null;
            ResultSet resultSet = null;
            try {
                final String validationQuery = "select 1";
                statement = connection.createStatement();
                statement.setQueryTimeout(timeOut);
                resultSet = statement.executeQuery(validationQuery);
                if (!resultSet.next()) {
                    return false;
                }
            } finally {
                JdbcUtil.closeDbResources(resultSet, statement, null, false);
            }
        } catch (Throwable e) {
            return false;
        }
        return true;
    }

    public String buildLocation(
            String columnType, String location, Function<Long, String> function) {
        if (ColumnType.isTimeType(columnType)) {
            if (ColumnType.TIMESTAMP.name().equalsIgnoreCase(columnType)) {
                return location;
            }
            return function.apply(Long.parseLong(location));
        } else if (ColumnType.isNumberType(columnType)) {
            return location;
        } else {
            return "'" + location + "'";
        }
    }
}
