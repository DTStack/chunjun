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
package com.dtstack.chunjun.metrics.rdb;

import com.dtstack.chunjun.util.ClassUtil;
import com.dtstack.chunjun.util.ExceptionUtil;
import com.dtstack.chunjun.util.RetryUtil;
import com.dtstack.chunjun.util.TelnetUtil;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

@Slf4j
public class JdbcUtil {

    public static final String TEMPORARY_TABLE_NAME = "chunjun_tmp";

    /**
     * 获取JDBC连接
     *
     * @param jdbcConfig
     * @param jdbcDialect
     * @return
     */
    public static Connection getConnection(JdbcMetricConf jdbcConfig, JdbcDialect jdbcDialect) {
        TelnetUtil.telnet(jdbcConfig.getJdbcUrl());
        ClassUtil.forName(
                jdbcDialect.defaultDriverName().get(),
                Thread.currentThread().getContextClassLoader());
        Map<String, String> properties = jdbcConfig.getProperties();
        Properties prop = new Properties();
        if (MapUtils.isNotEmpty(properties)) {
            for (final Map.Entry<String, String> entry : properties.entrySet()) {
                prop.setProperty(entry.getKey(), entry.getValue());
            }
        }
        if (StringUtils.isNotBlank(jdbcConfig.getUsername())) {
            prop.put("user", jdbcConfig.getUsername());
        }
        if (StringUtils.isNotBlank(jdbcConfig.getPassword())) {
            prop.put("password", jdbcConfig.getPassword());
        }
        synchronized (ClassUtil.LOCK_STR) {
            return RetryUtil.executeWithRetry(
                    () -> DriverManager.getConnection(jdbcConfig.getJdbcUrl(), prop),
                    3,
                    2000,
                    false);
        }
    }

    /**
     * 手动提交事物
     *
     * @param conn Connection
     */
    public static void commit(Connection conn) {
        try {
            if (null != conn && !conn.isClosed() && !conn.getAutoCommit()) {
                conn.commit();
            }
        } catch (SQLException e) {
            log.warn("commit error:{}", ExceptionUtil.getErrorMessage(e));
        }
    }
}
