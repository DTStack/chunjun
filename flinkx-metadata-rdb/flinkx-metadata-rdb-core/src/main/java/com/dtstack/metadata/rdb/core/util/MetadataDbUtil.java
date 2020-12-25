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

import com.dtstack.flinkx.constants.ConstantValue;
import com.dtstack.flinkx.util.ClassUtil;
import com.dtstack.flinkx.util.ExceptionUtil;
import com.dtstack.flinkx.util.SysUtil;
import com.dtstack.flinkx.util.TelnetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author kunni@dtstack.com
 */
public class MetadataDbUtil {

    private static final Logger LOG = LoggerFactory.getLogger(MetadataDbUtil.class);

    private static int MAX_RETRY_TIMES = 3;

    public static void closeConnection(Connection connection){
        if(connection != null){
            try{
                connection.close();
            }catch (SQLException e){
                LOG.error("failed to close connection , cause = {} ", ExceptionUtil.getErrorMessage(e));
            }
        }
    }

    /**
     * 获取jdbc连接(重试3次)
     * @param url       url
     * @param username  账号
     * @param password  密码
     * @return connection
     * @throws SQLException sql异常
     */
    public static Connection getConnection(String url, String username, String password) throws SQLException {
        if (!url.startsWith(ConstantValue.PROTOCOL_JDBC_MYSQL)) {
            return getConnectionInternal(url, username, password);
        } else {
            boolean failed = true;
            Connection dbConn = null;
            for (int i = 0; i < MAX_RETRY_TIMES && failed; ++i) {
                try {
                    dbConn = getConnectionInternal(url, username, password);
                    try (Statement statement = dbConn.createStatement()){
                        statement.execute("SELECT 1 FROM dual");
                        failed = false;
                    }
                } catch (Exception e) {
                    if (dbConn != null) {
                        dbConn.close();
                    }

                    if (i == MAX_RETRY_TIMES - 1) {
                        throw e;
                    } else {
                        SysUtil.sleep(3000);
                    }
                }
            }

            return dbConn;
        }
    }



    /**
     * 获取jdbc连接(超时10S)
     * @param url       url
     * @param username  账号
     * @param password  密码
     * @return Connection
     * @throws SQLException sql异常
     */
    private static Connection getConnectionInternal(String url, String username, String password) throws SQLException {
        Connection dbConn;
        synchronized (ClassUtil.LOCK_STR){
            DriverManager.setLoginTimeout(10);

            // telnet
            TelnetUtil.telnet(url);

            if (username == null) {
                dbConn = DriverManager.getConnection(url);
            } else {
                dbConn = DriverManager.getConnection(url, username, password);
            }
        }

        return dbConn;
    }

}
