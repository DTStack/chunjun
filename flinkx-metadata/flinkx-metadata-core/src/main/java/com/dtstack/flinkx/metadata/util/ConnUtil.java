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
package com.dtstack.flinkx.metadata.util;

import com.dtstack.flinkx.util.ClassUtil;
import com.dtstack.flinkx.util.ExceptionUtil;
import com.dtstack.flinkx.util.SysUtil;
import com.dtstack.flinkx.util.TelnetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.regex.Pattern;

/**
 * @author : tiezhu
 * @date : 2020/3/8
 * @description :
 */
public class ConnUtil {

    private static final Logger LOG = LoggerFactory.getLogger(ConnUtil.class);

    /**
     * 数据库连接的最大重试次数
     */
    private static int MAX_RETRY_TIMES = 3;

    /**
     * 秒级时间戳的长度为10位
     */
    private static int SECOND_LENGTH = 10;
    /**
     * 毫秒级时间戳的长度为13位
     */
    private static int MILLIS_LENGTH = 13;
    /**
     * 微秒级时间戳的长度为16位
     */
    private static int MICRO_LENGTH = 16;
    /**
     * 纳秒级时间戳的长度为19位
     */
    private static int NANOS_LENGTH = 19;

    /**
     * jdbc连接URL的分割正则，用于获取URL?后的连接参数
     */
    public static final Pattern DB_PATTERN = Pattern.compile("\\?");

    /**
     * 增量任务过滤条件占位符
     */
    public static final String INCREMENT_FILTER_PLACEHOLDER = "${incrementFilter}";

    /**
     * 断点续传过滤条件占位符
     */
    public static final String RESTORE_FILTER_PLACEHOLDER = "${restoreFilter}";

    public static final String TEMPORARY_TABLE_NAME = "flinkx_tmp";

    public static final String NULL_STRING = "null";

    /**
     * 获取jdbc连接(超时10S)
     * @param url       url
     * @param username  账号
     * @param password  密码
     * @return
     * @throws SQLException
     */
    private static Connection getConnectionInternal(String url, String username, String password) throws SQLException {
        Connection dbConn;
        synchronized (ClassUtil.lock_str){
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

    /**
     * 获取jdbc连接(重试3次)
     * @param url       url
     * @param username  账号
     * @param password  密码
     * @return
     * @throws SQLException
     */
    public static Connection getConnection(String url, String username, String password) throws SQLException {
        if (!url.startsWith("jdbc:mysql")) {
            return getConnectionInternal(url, username, password);
        } else {
            boolean failed = true;
            Connection dbConn = null;
            for (int i = 0; i < MAX_RETRY_TIMES && failed; ++i) {
                try {
                    dbConn = getConnectionInternal(url, username, password);
                    dbConn.createStatement().execute("select 111");
                    failed = false;
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
     * 关闭连接资源
     * @param rs        ResultSet
     * @param stmt      Statement
     * @param conn      Connection
     * @param commit
     */
    public static void closeDBResources(ResultSet rs, Statement stmt, Connection conn, boolean commit) {
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
                if(commit){
                    commit(conn);
                }

                conn.close();
            } catch (SQLException e) {
                LOG.warn("Close connection error:{}", ExceptionUtil.getErrorMessage(e));
            }
        }
    }

    /**
     * 手动提交事物
     * @param conn Connection
     */
    public static void commit(Connection conn){
        try {
            if (!conn.isClosed() && !conn.getAutoCommit()){
                conn.commit();
            }
        } catch (SQLException e){
            LOG.warn("commit error:{}", ExceptionUtil.getErrorMessage(e));
        }
    }
}
