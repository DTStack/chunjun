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

package com.dtstack.chunjun.connector.inceptor.util;

import com.dtstack.chunjun.connector.inceptor.conf.InceptorConf;
import com.dtstack.chunjun.connector.inceptor.dialect.InceptorDialect;
import com.dtstack.chunjun.connector.inceptor.dialect.InceptorHdfsDialect;
import com.dtstack.chunjun.connector.inceptor.dialect.InceptorHyperbaseDialect;
import com.dtstack.chunjun.connector.inceptor.dialect.InceptorSearchDialect;
import com.dtstack.chunjun.connector.jdbc.conf.JdbcConf;
import com.dtstack.chunjun.security.KerberosUtil;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;
import com.dtstack.chunjun.util.ExceptionUtil;
import com.dtstack.chunjun.util.FileSystemUtil;
import com.dtstack.chunjun.util.RetryUtil;

import org.apache.flink.api.common.cache.DistributedCache;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.PrivilegedAction;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author dujie
 * @version 1.0
 * @date 2021/12/15 16:44 星期三
 * @email wujuan@dtstack.com
 * @company www.dtstack.com
 */
public final class InceptorDbUtil {

    public static final String SQLSTATE_USERNAME_PWD_ERROR = "28000";
    public static final String SQLSTATE_CANNOT_ACQUIRE_CONNECT = "08004";
    public static final int JDBC_PART_SIZE = 2;
    public static final String JDBC_REGEX = "[?|;|#]";
    public static final String KEY_VAL_DELIMITER = "=";
    public static final String PARAM_DELIMITER = "&";
    public static final String KEY_PRINCIPAL = "principal";
    private static final Logger LOG = LoggerFactory.getLogger(InceptorDbUtil.class);
    private static final ReentrantLock lock = new ReentrantLock();

    public static final String INCEPTOR_TRANSACTION_TYPE = "set transaction.type=inceptor";
    public static final String INCEPTOR_TRANSACTION_BEGIN = "BEGIN TRANSACTION";
    public static final String INCEPTOR_TRANSACTION_COMMIT = "COMMIT";
    public static final String INCEPTOR_TRANSACTION_ROLLBACK = "ROLLBACK";

    public static final String INCEPTOR_HYPERBASE_STORAGE_HANDLER = "HyperdriveStorageHandler";
    public static final String INCEPTOR_HBASE_STORAGE_HANDLER = "HBaseStorageHandler";
    public static final String INCEPTOR_SEARCH_STORAGE_HANDLER = "ElasticSearchStorageHandler";

    public static final String INCEPROE_SHOW_DESCRIBE_FORMAT = "describe formatted %s";

    public static final ThreadLocal<TimeZone> LOCAL_TIMEZONE =
            new ThreadLocal<TimeZone>() {
                protected TimeZone initialValue() {
                    return Calendar.getInstance().getTimeZone();
                }
            };

    private InceptorDbUtil() {}

    public static Connection getConnection(
            InceptorConf connectionInfo, DistributedCache distributedCache, String jobId) {
        if (openKerberos(connectionInfo.getJdbcUrl())) {
            return getConnectionWithKerberos(connectionInfo, distributedCache, jobId);
        } else {
            return getConnectionWithRetry(connectionInfo);
        }
    }

    private static Connection getConnectionWithRetry(InceptorConf connectionInfo) {
        try {
            return RetryUtil.executeWithRetry(() -> connect(connectionInfo), 1, 1000L, false);
        } catch (Exception e1) {
            throw new RuntimeException(
                    String.format(
                            "连接：%s 时发生错误：%s.",
                            connectionInfo.getJdbcUrl(), ExceptionUtil.getErrorMessage(e1)));
        }
    }

    private static Connection getConnectionWithKerberos(
            InceptorConf connectionInfo, DistributedCache distributedCache, String jobId) {

        String keytabFileName = KerberosUtil.getPrincipalFileName(connectionInfo.getHadoopConfig());
        keytabFileName = KerberosUtil.loadFile(connectionInfo.getHadoopConfig(), keytabFileName);

        String principal =
                KerberosUtil.getPrincipal(connectionInfo.getHadoopConfig(), keytabFileName);
        KerberosUtil.loadKrb5Conf(connectionInfo.getHadoopConfig(), distributedCache);

        Configuration conf =
                FileSystemUtil.getConfiguration(connectionInfo.getHadoopConfig(), null);

        UserGroupInformation ugi;
        try {
            ugi = KerberosUtil.loginAndReturnUgi(conf, principal, keytabFileName);
        } catch (Exception e) {
            throw new RuntimeException("Login kerberos error:", e);
        }

        LOG.info("current ugi:{}", ugi);
        return ugi.doAs(
                (PrivilegedAction<Connection>) () -> getConnectionWithRetry(connectionInfo));
    }

    private static boolean openKerberos(final String jdbcUrl) {
        String[] splits = jdbcUrl.split(JDBC_REGEX);
        if (splits.length != JDBC_PART_SIZE) {
            return false;
        }

        String paramsStr = splits[1];
        String[] paramArray = paramsStr.split(PARAM_DELIMITER);
        for (String param : paramArray) {
            String[] keyVal = param.split(KEY_VAL_DELIMITER);
            if (KEY_PRINCIPAL.equalsIgnoreCase(keyVal[0])) {
                return true;
            }
        }

        return false;
    }

    public static Connection connect(InceptorConf connectionInfo) {
        lock.lock();
        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
            DriverManager.setLoginTimeout(connectionInfo.getQueryTimeOut());
            if (StringUtils.isNotBlank(connectionInfo.getUsername())) {
                return DriverManager.getConnection(
                        connectionInfo.getJdbcUrl(),
                        connectionInfo.getUsername(),
                        connectionInfo.getPassword());
            } else {
                return DriverManager.getConnection(connectionInfo.getJdbcUrl());
            }
        } catch (SQLException e) {
            if (SQLSTATE_USERNAME_PWD_ERROR.equals(e.getSQLState())) {
                throw new RuntimeException("用户名或密码错误.");
            } else if (SQLSTATE_CANNOT_ACQUIRE_CONNECT.equals(e.getSQLState())) {
                throw new RuntimeException("应用程序服务器拒绝建立连接.");
            } else {
                throw new RuntimeException(
                        "连接信息："
                                + connectionInfo.getJdbcUrl()
                                + " 错误信息："
                                + ExceptionUtil.getErrorMessage(e));
            }
        } catch (Exception e1) {
            throw new RuntimeException(
                    "连接信息："
                            + connectionInfo.getJdbcUrl()
                            + " 错误信息："
                            + ExceptionUtil.getErrorMessage(e1));
        } finally {
            lock.unlock();
        }
    }

    public static InceptorDialect getDialectWithDriverType(JdbcConf jdbcConf) {
        String storageHandler = getTableStorageHandler(jdbcConf);
        storageHandler = storageHandler.substring(storageHandler.lastIndexOf(".") + 1);
        switch (storageHandler) {
            case INCEPTOR_HYPERBASE_STORAGE_HANDLER:
            case INCEPTOR_HBASE_STORAGE_HANDLER:
                return new InceptorHyperbaseDialect();
            case INCEPTOR_SEARCH_STORAGE_HANDLER:
                return new InceptorSearchDialect();
            default:
                return new InceptorHdfsDialect();
        }
    }

    public static String getTableStorageHandler(JdbcConf jdbcConf) {
        Connection connection = getConnection((InceptorConf) jdbcConf, null, null);
        String schema = jdbcConf.getSchema();
        String table = jdbcConf.getTable();

        String storageHandler = "";
        try {
            if (StringUtils.isNotBlank(schema)) {
                connection.createStatement().execute(String.format("use %s", schema));
            }
            String sql = String.format(INCEPROE_SHOW_DESCRIBE_FORMAT, table);
            ResultSet resultSet = connection.createStatement().executeQuery(sql);
            while (resultSet.next()) {
                if (resultSet.getString(1).trim().equalsIgnoreCase("storage_handler")) {
                    storageHandler = resultSet.getString(2);
                    break;
                }
            }
            return storageHandler;
        } catch (SQLException e) {
            throw new ChunJunRuntimeException(
                    String.format("failed to get table[%s] storage handler", table), e);
        } finally {
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                if (connection != null) {
                    connection = null;
                }
            }
        }
    }
}
