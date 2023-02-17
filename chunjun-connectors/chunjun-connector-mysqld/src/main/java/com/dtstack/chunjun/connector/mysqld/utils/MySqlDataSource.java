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
package com.dtstack.chunjun.connector.mysqld.utils;

import com.alibaba.druid.pool.DruidDataSource;

import javax.sql.DataSource;

import java.sql.SQLException;
import java.util.Collections;

public class MySqlDataSource {

    private static final String DRIVER = "com.mysql.jdbc.Driver";

    public static DataSource getDataSource(String jdbcUrl, String username, String password)
            throws SQLException {
        DruidDataSource dataSource = new DruidDataSource();
        dataSource.setDriverClassName(DRIVER);
        dataSource.setUrl(jdbcUrl);
        dataSource.setUsername(username);
        dataSource.setPassword(password);
        dataSource.setInitialSize(1);
        dataSource.setMinIdle(1);
        dataSource.setMaxActive(5);
        dataSource.setMaxWait(30000);
        dataSource.setTimeBetweenEvictionRunsMillis(60000);
        dataSource.setMinEvictableIdleTimeMillis(300000);
        dataSource.setTestWhileIdle(true);
        dataSource.setTestOnBorrow(false);
        dataSource.setTestOnReturn(false);
        dataSource.setKeepAlive(true);
        dataSource.setPoolPreparedStatements(false);
        dataSource.setConnectionInitSqls(Collections.singletonList("set names 'utf8'"));

        dataSource.setRemoveAbandoned(false);
        dataSource.setLogAbandoned(true);
        dataSource.setTimeBetweenConnectErrorMillis(60000);
        dataSource.setConnectionErrorRetryAttempts(3);
        dataSource.init();

        return dataSource;
    }
}
