/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.chunjun.connector.clickhouse.util;

import com.dtstack.chunjun.util.ThreadUtil;

import ru.yandex.clickhouse.BalancedClickhouseDataSource;
import ru.yandex.clickhouse.settings.ClickHouseQueryParam;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class ClickhouseUtil {
    private static final int MAX_RETRY_TIMES = 3;

    public static Connection getConnection(String url, String username, String password)
            throws SQLException {
        Properties properties = new Properties();
        if (null != username) {
            properties.put(ClickHouseQueryParam.USER.getKey(), username);
        }

        if (null != password) {
            properties.put(ClickHouseQueryParam.PASSWORD.getKey(), password);
        }
        boolean failed = true;
        Connection conn = null;
        for (int i = 0; i < MAX_RETRY_TIMES && failed; ++i) {
            try {
                conn = new BalancedClickhouseDataSource(url, properties).getConnection();
                try (Statement statement = conn.createStatement()) {
                    statement.execute("select 111");
                    failed = false;
                }
            } catch (Exception e) {
                if (conn != null) {
                    conn.close();
                }
                if (i == MAX_RETRY_TIMES - 1) {
                    throw e;
                } else {
                    ThreadUtil.sleepMilliseconds(3000);
                }
            }
        }
        return conn;
    }
}
