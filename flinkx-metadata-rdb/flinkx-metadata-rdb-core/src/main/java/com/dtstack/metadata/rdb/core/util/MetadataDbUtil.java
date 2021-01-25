/*
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


package com.dtstack.metadata.rdb.core.util;

import com.dtstack.flinkx.util.ClassUtil;
import com.dtstack.flinkx.util.TelnetUtil;
import com.dtstack.metadata.rdb.core.entity.ConnectionInfo;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Objects;

/**
 * @author kunni@dtstack.com
 */
public class MetadataDbUtil {

    private static final Logger LOG = LoggerFactory.getLogger(MetadataDbUtil.class);

    /**
     * 关闭jdbc相关资源
     * @param closeables
     * @throws Exception
     */
    public static void close(AutoCloseable... closeables) throws Exception {
        if (Objects.nonNull(closeables)) {
            for (AutoCloseable closeable : closeables) {
                if (Objects.nonNull(closeable)) {
                    closeable.close();
                }
            }
        }
    }

    /**
     * 默认的连接方式
     * @param connectionInfo
     * @return
     * @throws SQLException
     */
    public static Connection getConnection(ConnectionInfo connectionInfo) throws SQLException {
        ClassUtil.forName(connectionInfo.getDriver());
        Connection dbConn;
        synchronized (ClassUtil.LOCK_STR) {
            DriverManager.setLoginTimeout(10);

            // telnet
            TelnetUtil.telnet(connectionInfo.getJdbcUrl());

            if (StringUtils.isEmpty(connectionInfo.getUsername())) {
                dbConn = DriverManager.getConnection(connectionInfo.getJdbcUrl());
            } else {
                dbConn = DriverManager.getConnection(connectionInfo.getJdbcUrl(), connectionInfo.getUsername(), connectionInfo.getPassword());
            }
        }
        return dbConn;
    }
}
