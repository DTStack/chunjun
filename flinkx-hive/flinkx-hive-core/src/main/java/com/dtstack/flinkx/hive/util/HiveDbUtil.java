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

package com.dtstack.flinkx.hive.util;

import com.dtstack.flinkx.authenticate.KerberosUtil;
import com.dtstack.flinkx.util.ExceptionUtil;
import com.dtstack.flinkx.util.FileSystemUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.collections.MapUtils;
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
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author toutian
 */
public final class HiveDbUtil {

    private static Logger LOG = LoggerFactory.getLogger(HiveDbUtil.class);

    public static final String SQLSTATE_USERNAME_PWD_ERROR = "28000";

    public static final String SQLSTATE_CANNOT_ACQUIRE_CONNECT = "08004";

    public static final String JDBC_REGEX = "[\\?|;|#]";
    public static final String KEY_VAL_DELIMITER = "=";
    public static final String PARAM_DELIMITER = "&";
    public static final String KEY_PRINCIPAL = "principal";

    public static Pattern HIVE_JDBC_PATTERN = Pattern.compile("(?i)jdbc:hive2://(?<host>[0-9a-zA-Z\\.]+):(?<port>\\d+)/(?<db>[0-9a-z_%]+)(?<param>[\\?;#].*)*");
    public static final String HOST_KEY = "host";
    public static final String PORT_KEY = "port";
    public static final String DB_KEY = "db";
    public static final String PARAM_KEY = "param";
    public static final String HIVE_SERVER2_AUTHENTICATION_KERBEROS_KEYTAB_KEY = "hive.server2.authentication.kerberos.keytab";

    private static final String ERROR_MSG_NO_DB = "NoSuchDatabaseException";

    private static ReentrantLock lock = new ReentrantLock();

    private HiveDbUtil() {
    }

    public static Connection getConnection(ConnectionInfo connectionInfo) {
        if(openKerberos(connectionInfo.getJdbcUrl())){
            return getConnectionWithKerberos(connectionInfo);
        } else {
            return getConnectionWithRetry(connectionInfo);
        }
    }

    private static Connection getConnectionWithRetry(ConnectionInfo connectionInfo){
        try {
            return RetryUtil.executeWithRetry(new Callable<Connection>() {
                @Override
                public Connection call() throws Exception {
                    return HiveDbUtil.connect(connectionInfo);
                }
            }, 1, 1000L, false);
        } catch (Exception e1) {
            throw new RuntimeException(String.format("连接：%s 时发生错误：%s.", connectionInfo.getJdbcUrl(), ExceptionUtil.getErrorMessage(e1)));
        }
    }

    private static Connection getConnectionWithKerberos(ConnectionInfo connectionInfo){
        if(connectionInfo.getHiveConf() == null || connectionInfo.getHiveConf().isEmpty()){
            throw new IllegalArgumentException("hiveConf can not be null or empty");
        }

        String keytabFileName = KerberosUtil.getPrincipalFileName(connectionInfo.getHiveConf());

        keytabFileName = KerberosUtil.loadFile(connectionInfo.getHiveConf(), keytabFileName);
        String principal = KerberosUtil.findPrincipalFromKeytab(keytabFileName);
        KerberosUtil.loadKrb5Conf(connectionInfo.getHiveConf());

        Configuration conf = FileSystemUtil.getConfiguration(connectionInfo.getHiveConf(), null);

        UserGroupInformation ugi;
        try {
            ugi = KerberosUtil.loginAndReturnUgi(conf, principal, keytabFileName);
        } catch (Exception e){
            throw new RuntimeException("Login kerberos error:", e);
        }

        LOG.info("current ugi:{}", ugi);
        return ugi.doAs(new PrivilegedAction<Connection>() {
            @Override
            public Connection run(){
                return getConnectionWithRetry(connectionInfo);
            }
        });
    }

    private static boolean openKerberos(final String jdbcUrl){
        String[] splits = jdbcUrl.split(JDBC_REGEX);
        if (splits.length != 2) {
            return false;
        }

        String paramsStr = splits[1];
        String[] paramArray = paramsStr.split(PARAM_DELIMITER);
        for (String param : paramArray) {
            String[] keyVal = param.split(KEY_VAL_DELIMITER);
            if(KEY_PRINCIPAL.equalsIgnoreCase(keyVal[0])){
                return true;
            }
        }

        return false;
    }

    private static String getKeytab(Map<String, Object> hiveConf){
        String keytab = MapUtils.getString(hiveConf, KerberosUtil.KEY_PRINCIPAL_FILE);
        if(StringUtils.isEmpty(keytab)){
            keytab = MapUtils.getString(hiveConf, HIVE_SERVER2_AUTHENTICATION_KERBEROS_KEYTAB_KEY);
        }

        if(StringUtils.isNotEmpty(keytab)){
            return keytab;
        }

        throw new IllegalArgumentException("can not find keytab from hiveConf");
    }

    public static Connection connect(ConnectionInfo connectionInfo) {

        String addr = parseIpAndPort(connectionInfo.getJdbcUrl());
        String[] addrs = addr.split(":");
        boolean check;
        if (addrs.length == 1) {
            String ip = addrs[0].trim();
            check = AddressUtil.ping(ip);
        } else {
            String ip = addrs[0].trim();
            String port = addrs[1].trim();
            check = AddressUtil.telnet(ip, Integer.parseInt(port));
        }

        if (!check) {
            throw new RuntimeException("连接信息：" + connectionInfo.getJdbcUrl() + " 数据库服务器端口连接失败,请检查您的数据库配置或服务状态.");
        }

        Properties prop = new Properties();
        if(connectionInfo.getUsername() != null){
            prop.put("user", connectionInfo.getUsername());
        }

        if(connectionInfo.getPassword() != null){
            prop.put("password", connectionInfo.getPassword());
        }

        return connect(connectionInfo, prop);
    }

    private static Connection connect(ConnectionInfo connectionInfo, Properties prop) {
        lock.lock();
        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
            DriverManager.setLoginTimeout(connectionInfo.getTimeout());
            return getHiveConnection(connectionInfo.getJdbcUrl(), prop);
        } catch (SQLException e) {
            if (SQLSTATE_USERNAME_PWD_ERROR.equals(e.getSQLState())) {
                throw new RuntimeException("用户名或密码错误.");
            } else if (SQLSTATE_CANNOT_ACQUIRE_CONNECT.equals(e.getSQLState())) {
                throw new RuntimeException("应用程序服务器拒绝建立连接.");
            } else {
                throw new RuntimeException("连接信息：" + connectionInfo.getJdbcUrl() + " 错误信息：" + ExceptionUtil.getErrorMessage(e));
            }
        } catch (Exception e1) {
            throw new RuntimeException("连接信息：" + connectionInfo.getJdbcUrl() + " 错误信息：" + ExceptionUtil.getErrorMessage(e1));
        } finally {
            lock.unlock();
        }
    }

    /**
     * 获取hive连接
     *
     * @param url
     * @param prop
     * @return
     * @throws Exception
     */
    private static Connection getHiveConnection(String url, Properties prop) throws Exception {
        Matcher matcher = HIVE_JDBC_PATTERN.matcher(url);
        String db = null;
        String host = null;
        String port = null;
        String param = null;
        if (matcher.find()) {
            host = matcher.group(HOST_KEY);
            port = matcher.group(PORT_KEY);
            db = matcher.group(DB_KEY);
            param = matcher.group(PARAM_KEY);
        }

        if (StringUtils.isNotEmpty(host) && StringUtils.isNotEmpty(db)) {
            param = param == null ? "" : param;
            url = String.format("jdbc:hive2://%s:%s/%s", host, port, param);
            Connection connection = DriverManager.getConnection(url, prop);
            if (StringUtils.isNotEmpty(db)) {
                try {
                    connection.createStatement().execute("use " + db);
                } catch (SQLException e) {
                    if (connection != null) {
                        connection.close();
                    }

                    if (e.getMessage().contains(ERROR_MSG_NO_DB)) {
                        throw new RuntimeException(e.getMessage());
                    } else {
                        throw e;
                    }
                }
            }

            return connection;
        }

        throw new RuntimeException("jdbcUrl 不规范");
    }


    public static String parseIpAndPort(String url) {
        String addr;
        Matcher matcher = HIVE_JDBC_PATTERN.matcher(url);
        if (matcher.find()) {
            addr = matcher.group(HOST_KEY) + ":" + matcher.group(PORT_KEY);
        } else {
            addr = url.substring(url.indexOf("//") + 2, url.lastIndexOf("/"));
        }
        return addr;
    }

    public static List<Map<String, Object>> executeQuery(Connection connection, String sql) {
        List<Map<String, Object>> result = Lists.newArrayList();
        ResultSet res = null;
        Statement statement = null;
        try {
            statement = connection.createStatement();
            res = statement.executeQuery(sql);
            int columns = res.getMetaData().getColumnCount();
            List<String> columnName = Lists.newArrayList();
            for (int i = 0; i < columns; i++) {
                columnName.add(res.getMetaData().getColumnName(i + 1));
            }

            while (res.next()) {
                Map<String, Object> row = Maps.newLinkedHashMap();;
                for (int i = 0; i < columns; i++) {
                    row.put(columnName.get(i), res.getObject(i + 1));
                }
                result.add(row);
            }
        } catch (Exception e) {
            throw new RuntimeException("SQL 执行异常");
        } finally {
            HiveDbUtil.closeDbResources(res, statement, null);
        }
        return result;
    }

    public static boolean executeSqlWithoutResultSet(ConnectionInfo connectionInfo, Connection connection, String sql) {
        boolean flag = true;
        Statement statement = null;
        try {
            statement = connection.createStatement();
            statement.setQueryTimeout(connectionInfo.getTimeout());
            executeSqlWithoutResultSet(statement, sql);
        } catch (Exception e) {
            flag = false;
            throw new RuntimeException(String.format("execute sql:%s, errorMessage:[%s]", sql, e.getMessage()));
        } finally {
            HiveDbUtil.closeDbResources(null, statement, null);
        }

        return flag;
    }

    private static void executeSqlWithoutResultSet(Statement stmt, String sql)
            throws SQLException {
        stmt.execute(sql);
    }

    public static void closeDbResources(ResultSet rs, Statement stmt,
                                        Connection conn) {
        try {
            if (null != rs) {
                rs.close();
            }

            if (null != stmt) {
                stmt.close();
            }

            if (null != conn) {
                conn.close();
            }
        } catch (Throwable t) {
            LOG.warn("", t);
        }
    }

    public static class ConnectionInfo{
        private String jdbcUrl;
        private String username;
        private String password;
        private int timeout = 30000;
        private Map<String, Object> hiveConf;

        public String getJdbcUrl() {
            return jdbcUrl;
        }

        public void setJdbcUrl(String jdbcUrl) {
            this.jdbcUrl = jdbcUrl;
        }

        public String getUsername() {
            return username;
        }

        public void setUsername(String username) {
            this.username = username;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }

        public Map<String, Object> getHiveConf() {
            return hiveConf;
        }

        public void setHiveConf(Map<String, Object> hiveConf) {
            this.hiveConf = hiveConf;
        }

        public int getTimeout() {
            return timeout;
        }

        public void setTimeout(int timeout) {
            this.timeout = timeout;
        }

        @Override
        public String toString() {
            return "ConnectionInfo{" +
                    "jdbcUrl='" + jdbcUrl + '\'' +
                    ", username='" + username + '\'' +
                    ", password='" + password + '\'' +
                    ", timeout='" + timeout + '\'' +
                    ", hiveConf=" + hiveConf +
                    '}';
        }
    }
}
