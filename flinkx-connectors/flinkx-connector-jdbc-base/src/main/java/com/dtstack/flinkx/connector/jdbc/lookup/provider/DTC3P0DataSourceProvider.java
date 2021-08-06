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

package com.dtstack.flinkx.connector.jdbc.lookup.provider;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.spi.impl.C3P0DataSourceProvider;

import javax.sql.DataSource;

import java.beans.PropertyVetoException;
import java.sql.SQLException;

/**
 * Date: 2019/9/17 Company: www.dtstack.com
 *
 * @author maqi
 */
public class DTC3P0DataSourceProvider extends C3P0DataSourceProvider {

    @Override
    public DataSource getDataSource(JsonObject config) throws SQLException {
        String url = config.getString("url");
        if (url == null) {
            throw new NullPointerException("url cannot be null");
        }
        String driverClass = config.getString("driver_class");
        String user = config.getString("user");
        String password = config.getString("password");
        Integer maxPoolSize = config.getInteger("max_pool_size");
        Integer initialPoolSize = config.getInteger("initial_pool_size");
        Integer minPoolSize = config.getInteger("min_pool_size");
        Integer maxStatements = config.getInteger("max_statements");
        Integer maxStatementsPerConnection = config.getInteger("max_statements_per_connection");
        Integer maxIdleTime = config.getInteger("max_idle_time");
        Integer acquireRetryAttempts = config.getInteger("acquire_retry_attempts");
        Integer acquireRetryDelay = config.getInteger("acquire_retry_delay");
        Boolean breakAfterAcquireFailure = config.getBoolean("break_after_acquire_failure");

        // add c3p0 params
        String preferredTestQuery = config.getString("preferred_test_query");
        Integer idleConnectionTestPeriod = config.getInteger("idle_connection_test_period");
        Boolean testConnectionOnCheckin = config.getBoolean("test_connection_on_checkin");

        // If you want to configure any other C3P0 properties you can add a file c3p0.properties to
        // the classpath
        ComboPooledDataSource cpds = new ComboPooledDataSource();
        cpds.setJdbcUrl(url);
        if (driverClass != null) {
            try {
                cpds.setDriverClass(driverClass);
            } catch (PropertyVetoException e) {
                throw new IllegalArgumentException(e);
            }
        }
        if (user != null) {
            cpds.setUser(user);
        }
        if (password != null) {
            cpds.setPassword(password);
        }
        if (maxPoolSize != null) {
            cpds.setMaxPoolSize(maxPoolSize);
        }
        if (minPoolSize != null) {
            cpds.setMinPoolSize(minPoolSize);
        }
        if (initialPoolSize != null) {
            cpds.setInitialPoolSize(initialPoolSize);
        }
        if (maxStatements != null) {
            cpds.setMaxStatements(maxStatements);
        }
        if (maxStatementsPerConnection != null) {
            cpds.setMaxStatementsPerConnection(maxStatementsPerConnection);
        }
        if (maxIdleTime != null) {
            cpds.setMaxIdleTime(maxIdleTime);
        }
        if (acquireRetryAttempts != null) {
            cpds.setAcquireRetryAttempts(acquireRetryAttempts);
        }
        if (acquireRetryDelay != null) {
            cpds.setAcquireRetryDelay(acquireRetryDelay);
        }
        if (breakAfterAcquireFailure != null) {
            cpds.setBreakAfterAcquireFailure(breakAfterAcquireFailure);
        }

        if (preferredTestQuery != null) {
            cpds.setPreferredTestQuery(preferredTestQuery);
        }

        if (idleConnectionTestPeriod != null) {
            cpds.setIdleConnectionTestPeriod(idleConnectionTestPeriod);
        }

        if (testConnectionOnCheckin != null) {
            cpds.setTestConnectionOnCheckin(testConnectionOnCheckin);
        }

        return cpds;
    }
}
