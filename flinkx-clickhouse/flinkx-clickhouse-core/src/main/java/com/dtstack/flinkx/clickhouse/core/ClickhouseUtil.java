/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dtstack.flinkx.clickhouse.core;

import com.dtstack.flinkx.util.ExceptionUtil;
import com.dtstack.flinkx.util.SysUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.clickhouse.BalancedClickhouseDataSource;
import ru.yandex.clickhouse.settings.ClickHouseProperties;
import ru.yandex.clickhouse.settings.ClickHouseQueryParam;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 * Date: 2019/11/05
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public class ClickhouseUtil {
    private static final Logger LOG = LoggerFactory.getLogger(ClickhouseUtil.class);

    private static final int MAX_RETRY_TIMES = 3;

    public static Connection getConnection(String url, String username, String password, Properties properties) throws SQLException {
        properties.put(ClickHouseQueryParam.USER, username);
        properties.put(ClickHouseQueryParam.PASSWORD, password);
        boolean failed = true;
        Connection conn = null;
        for (int i = 0; i < MAX_RETRY_TIMES && failed; ++i) {
            try {
                conn = new BalancedClickhouseDataSource(url, properties).getConnection();
                conn.createStatement().execute("select 111");
                failed = false;
            } catch (Exception e) {
                if (conn != null) {
                    conn.close();
                }
                if (i == MAX_RETRY_TIMES - 1) {
                    throw e;
                } else {
                    SysUtil.sleep(3000);
                }
            }
        }

        return conn;
    }

    public static void closeDBResources(ResultSet rs, Statement stmt, Connection conn) {
        if (null != rs) {
            try {
                rs.close();
            } catch (SQLException e) {
                LOG.warn("Close resultSet error: {}", ExceptionUtil.getErrorMessage(e));
            }
        }

        if (null != stmt) {
            try {
                stmt.close();
            } catch (SQLException e) {
                LOG.warn("Close statement error:{}", ExceptionUtil.getErrorMessage(e));
            }
        }

        if (null != conn) {
            try {
                conn.close();
            } catch (SQLException e) {
                LOG.warn("Close connection error:{}", ExceptionUtil.getErrorMessage(e));
            }
        }
    }

    public static Object getValue(ResultSet rs, String name, String type) throws SQLException {
        switch (type.toLowerCase()) {
            case "UInt64":
                return rs.getBigDecimal(name);
            case "UInt32":
            case "Int64":
                return rs.getLong(name);
            case "IntervalYear":
            case "IntervalQuarter":
            case "IntervalMonth":
            case "IntervalWeek":
            case "IntervalDay":
            case "IntervalHour":
            case "IntervalMinute":
            case "IntervalSecond":
            case "UInt8":
            case "Int32":
            case "UInt16":
            case "Int16":
                return rs.getInt(name);
            default:
                return rs.getString(name);
        }
    }
}
