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

package com.dtstack.chunjun.connector.starrocks.connection;

import com.dtstack.chunjun.util.ClassUtil;
import com.dtstack.chunjun.util.RetryUtil;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/** Simple JDBC connection provider. */
@Slf4j
public class StarRocksJdbcConnectionProvider
        implements StarRocksJdbcConnectionIProvider, Serializable {

    private static final long serialVersionUID = 5487215248897952765L;

    private final StarRocksJdbcConnectionOptions jdbcOptions;

    private transient volatile Connection connection;

    public StarRocksJdbcConnectionProvider(StarRocksJdbcConnectionOptions jdbcOptions) {
        this.jdbcOptions = jdbcOptions;
    }

    @Override
    public Connection getConnection() throws ClassNotFoundException {
        if (connection == null) {
            synchronized (this) {
                if (connection == null) {
                    try {
                        Class.forName(jdbcOptions.getCjDriverName());
                    } catch (ClassNotFoundException ex) {
                        Class.forName(jdbcOptions.getDriverName());
                    }
                    Properties prop = new Properties();
                    if (StringUtils.isNotBlank(jdbcOptions.getUsername().orElse(null))) {
                        prop.put("user", jdbcOptions.getUsername().get());
                    }
                    if (StringUtils.isNotBlank(jdbcOptions.getPassword().orElse(null))) {
                        prop.put("password", jdbcOptions.getPassword().get());
                    }
                    synchronized (ClassUtil.LOCK_STR) {
                        connection =
                                RetryUtil.executeWithRetry(
                                        () ->
                                                DriverManager.getConnection(
                                                        jdbcOptions.getDbURL(), prop),
                                        3,
                                        2000,
                                        false);
                    }
                }
            }
        }
        return connection;
    }

    public void checkValid() throws SQLException, ClassNotFoundException {
        if (connection == null || !connection.isValid(10)) {
            connection = null;
            getConnection();
        }
    }

    @Override
    public Connection reestablishConnection() throws ClassNotFoundException {
        close();
        connection = getConnection();
        return connection;
    }

    @Override
    public void close() {
        if (connection == null) {
            return;
        }
        try {
            connection.close();
        } catch (SQLException e) {
            log.error("JDBC connection close failed.", e);
        } finally {
            connection = null;
        }
    }
}
