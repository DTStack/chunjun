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

package com.dtstack.flinkx.metadatahive2.constants;

import com.dtstack.flinkx.authenticate.KerberosUtil;
import com.dtstack.flinkx.util.ExceptionUtil;
import com.dtstack.flinkx.util.FileSystemUtil;
import com.dtstack.flinkx.util.RetryUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.net.telnet.TelnetClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.security.PrivilegedAction;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;
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

    public static final int JDBC_PART_SIZE = 2;

    public static final String JDBC_REGEX = "[?|;|#]";
    public static final String KEY_VAL_DELIMITER = "=";
    public static final String PARAM_DELIMITER = "&";
    public static final String KEY_PRINCIPAL = "principal";

    public static Pattern HIVE_JDBC_PATTERN = Pattern.compile("(?i)jdbc:hive2://(?<host>[0-9a-zA-Z.]+):(?<port>\\d+)/(?<db>[0-9a-z_%]+)(?<param>[?;#].*)*");
    public static final String HOST_KEY = "host";
    public static final String PORT_KEY = "port";
    public static final String DB_KEY = "db";
    public static final String PARAM_KEY = "param";

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
            return RetryUtil.executeWithRetry(() -> connect(connectionInfo), 1, 1000L, false);
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
        String principal = KerberosUtil.getPrincipal(connectionInfo.getHiveConf(), keytabFileName);
        KerberosUtil.loadKrb5Conf(connectionInfo.getHiveConf());

        Configuration conf = FileSystemUtil.getConfiguration(connectionInfo.getHiveConf(), null);

        UserGroupInformation ugi;
        try {
            ugi = KerberosUtil.loginAndReturnUgi(conf, principal, keytabFileName);
        } catch (Exception e){
            throw new RuntimeException("Login kerberos error:", e);
        }

        LOG.info("current ugi:{}", ugi);
        return ugi.doAs((PrivilegedAction<Connection>) () -> getConnectionWithRetry(connectionInfo));
    }

    private static boolean openKerberos(final String jdbcUrl){
        String[] splits = jdbcUrl.split(JDBC_REGEX);
        if (splits.length != JDBC_PART_SIZE) {
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

    public static Connection connect(ConnectionInfo connectionInfo) {

        String addr = parseIpAndPort(connectionInfo.getJdbcUrl());
        String[] addrs = addr.split(":");
        boolean check;
        String ip = addrs[0].trim();
        if (addrs.length == 1) {
            check = AddressUtil.ping(ip);
        } else {
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
            Class.forName(connectionInfo.getDriver());
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
     * @param url jdbcUrl
     * @param prop Properties
     * @return Connection
     * @throws Exception RuntimeException
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
                try (Statement statement = connection.createStatement()) {
                    statement.execute("use " + db);
                } catch (SQLException e) {
                    connection.close();

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

    public static class ConnectionInfo{
        private String jdbcUrl;
        private String username;
        private String password;
        private String driver;
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

        public void setDriver(String driver){
            this.driver = driver;
        }

        public String getDriver(){
            return driver;
        }

        @Override
        public String toString() {
            return "ConnectionInfo{" +
                    "jdbcUrl='" + jdbcUrl + '\'' +
                    ", username='" + username + '\'' +
                    ", password='" + password + '\'' +
                    ", timeout='" + timeout + '\'' +
                    ", driver='" + driver + '\'' +
                    ", hiveConf=" + hiveConf +
                    '}';
        }
    }

    public static class AddressUtil {

        private static Logger logger = LoggerFactory.getLogger(AddressUtil.class);

        public static boolean telnet(String ip,int port){
            TelnetClient client = null;
            try{
                client = new TelnetClient();
                client.setConnectTimeout(3000);
                client.connect(ip,port);
                return true;
            }catch(Exception e){
                return false;
            } finally {
                try {
                    if (client != null){
                        client.disconnect();
                    }
                } catch (Exception e){
                    logger.error(ExceptionUtil.getErrorMessage(e));
                }
            }
        }

        public static boolean ping(String ip){
            try{
                return InetAddress.getByName(ip).isReachable(3000);
            }catch(Exception e){
                return false;
            }
        }
    }

}
