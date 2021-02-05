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


package com.dtstack.flinkx.oraclelogminer.format;

import com.dtstack.flinkx.oraclelogminer.entity.QueueData;
import com.dtstack.flinkx.oraclelogminer.util.SqlUtil;
import com.dtstack.flinkx.util.ClassUtil;
import com.dtstack.flinkx.util.ExceptionUtil;
import com.dtstack.flinkx.util.GsonUtil;
import com.dtstack.flinkx.util.RetryUtil;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang.time.DateFormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * @author jiangbo
 * @date 2020/3/27
 */
public class LogMinerConnection {

    public static final String KEY_PRIVILEGE = "PRIVILEGE";
    public static final String KEY_GRANTED_ROLE = "GRANTED_ROLE";
    public static final String DBA_ROLE = "DBA";
    public static final String EXECUTE_CATALOG_ROLE = "EXECUTE_CATALOG_ROLE";
    public static final int ORACLE_11_VERSION = 11;
    public static final List<String> PRIVILEGES_NEEDED = Arrays.asList(
            "CREATE SESSION",
            "LOGMINING",
            "SELECT ANY TRANSACTION",
            "SELECT ANY DICTIONARY");
    public static final List<String> ORACLE_11_PRIVILEGES_NEEDED = Arrays.asList(
            "CREATE SESSION",
            "SELECT ANY TRANSACTION",
            "SELECT ANY DICTIONARY");
    public static final int RETRY_TIMES = 3;
    public static final int SLEEP_TIME = 2000;
    public final static String KEY_SEG_OWNER = "SEG_OWNER";
    public final static String KEY_TABLE_NAME = "TABLE_NAME";
    public final static String KEY_OPERATION = "OPERATION";
    public final static String KEY_TIMESTAMP = "TIMESTAMP";
    public final static String KEY_SQL_REDO = "SQL_REDO";
    public final static String KEY_CSF = "CSF";
    public final static String KEY_SCN = "SCN";
    public final static String KEY_CURRENT_SCN = "CURRENT_SCN";
    public final static String KEY_FIRST_CHANGE = "FIRST_CHANGE#";
    private static final long QUERY_LOG_INTERVAL = 10000;
    public static Logger LOG = LoggerFactory.getLogger(LogMinerConnection.class);
    public int oracleVersion;
    //oracle10数据库字符编码是否设置为GBK
    public boolean isGBK = false;
    boolean isOracle10;
    private LogMinerConfig logMinerConfig;
    private Connection connection;
    private CallableStatement logMinerStartStmt;
    private PreparedStatement logMinerSelectStmt;
    private ResultSet logMinerData;
    private QueueData result;
    private List<LogFile> addedLogFiles = new ArrayList<>();
    private long lastQueryTime;
    private boolean logMinerStarted = false;

    /**
     * 上一次查询的scn
     */
    private Long preScn = null;

    public LogMinerConnection(LogMinerConfig logMinerConfig) {
        this.logMinerConfig = logMinerConfig;
    }

    public void connect() {
        PreparedStatement preparedStatement = null;
        try {
            ClassUtil.forName(logMinerConfig.getDriverName(), getClass().getClassLoader());

            connection = RetryUtil.executeWithRetry(() -> DriverManager.getConnection(logMinerConfig.getJdbcUrl(), logMinerConfig.getUsername(), logMinerConfig.getPassword()), RETRY_TIMES, SLEEP_TIME,false);

            oracleVersion = connection.getMetaData().getDatabaseMajorVersion();
            isOracle10 = oracleVersion == 10;

            if(isOracle10){
                //oracle10开启logMiner之前 需要设置会话级别的日期格式 否则sql语句会含有todate函数 而不是todate函数计算后的值
                preparedStatement = connection.prepareStatement(SqlUtil.SQL_ALTER_DATE_FORMAT);
                preparedStatement.execute();
                preparedStatement = connection.prepareStatement(SqlUtil.NLS_TIMESTAMP_FORMAT);
                preparedStatement.execute();
            }

           LOG.info("get connection successfully, url:{}, username:{}, Oracle version：{}", logMinerConfig.getJdbcUrl(), logMinerConfig.getUsername(), oracleVersion);
        } catch (Exception e){
            String message = String.format("get connection failed，url:[%s], username:[%s], e:%s", logMinerConfig.getJdbcUrl(), logMinerConfig.getUsername(), ExceptionUtil.getErrorMessage(e));
            LOG.error(message);
            closeResources(null, preparedStatement, connection);
            throw new RuntimeException(message, e);
        }
    }

    /**
     * 关闭LogMiner资源
     */
    public void disConnect() {
        //清除日志文件组，下次LogMiner启动时重新加载日志文件
        addedLogFiles.clear();

        if (null != logMinerStartStmt && logMinerStarted) {
            try {
                logMinerStartStmt.execute(SqlUtil.SQL_STOP_LOG_MINER);
            }catch (SQLException e){
                LOG.warn("close logMiner failed, e = {}", ExceptionUtil.getErrorMessage(e));
            }
            logMinerStarted = false;
        }

        closeStmt(logMinerStartStmt);
        closeResources(logMinerData, logMinerSelectStmt, connection);
    }

    /**
     * 启动LogMiner
     * @param startScn
     */
    public void startOrUpdateLogMiner(Long startScn) {
        String startSql = null;
        try {
            // 防止没有数据更新的时候频繁查询数据库，限定查询的最小时间间隔 QUERY_LOG_INTERVAL
            if (lastQueryTime > 0) {
                long time = System.currentTimeMillis() - lastQueryTime;
                if (time < QUERY_LOG_INTERVAL) {
                    try {
                        Thread.sleep(QUERY_LOG_INTERVAL-time);
                    } catch (InterruptedException e) {
                        LOG.warn("", e);
                    }
                }
            }
            lastQueryTime = System.currentTimeMillis();

            if (logMinerConfig.getSupportAutoAddLog()) {
                startSql = isOracle10 ? SqlUtil.SQL_START_LOG_MINER_AUTO_ADD_LOG_10 : SqlUtil.SQL_START_LOG_MINER_AUTO_ADD_LOG;
            } else {
                List<LogFile> newLogFiles = queryLogFiles(preScn);
                if (addedLogFiles.equals(newLogFiles)) {
                    return;
                } else {
                    LOG.info("Log group changed, new log group = {}", GsonUtil.GSON.toJson(newLogFiles));
                    addedLogFiles = newLogFiles;
                    startSql = isOracle10 ? SqlUtil.SQL_START_LOG_MINER_10 : SqlUtil.SQL_START_LOG_MINER;
                }
            }

            closeStmt(logMinerStartStmt);

            logMinerStartStmt = connection.prepareCall(startSql);
            configStatement(logMinerStartStmt);

            logMinerStartStmt.setLong(1, preScn);
            logMinerStartStmt.execute();

            logMinerStarted = true;
            LOG.info("start logMiner successfully, preScn:{}, startScn:{}", preScn, startScn);
            if(startScn > preScn){
                preScn = startScn;
            }
        } catch (SQLException e){
            String message = String.format("start logMiner failed, offset:[%s], sql:[%s], e: %s", startScn, startSql, ExceptionUtil.getErrorMessage(e));
            LOG.error(message);
            throw new RuntimeException(message, e);
        }
    }

    /**
     * 从LogMiner视图查询数据
     * @param startScn
     * @param logMinerSelectSql
     */
    public void queryData(Long startScn, String logMinerSelectSql) {
        try {
            logMinerSelectStmt = connection.prepareStatement(logMinerSelectSql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            configStatement(logMinerSelectStmt);

            logMinerSelectStmt.setFetchSize(logMinerConfig.getFetchSize());
            logMinerSelectStmt.setLong(1, startScn);
            logMinerData = logMinerSelectStmt.executeQuery();

            LOG.debug("query Log miner data, offset:{}", startScn);
        } catch (SQLException e) {
            String message = String.format("query logMiner data failed, sql:[%s], e: %s", logMinerSelectSql, ExceptionUtil.getErrorMessage(e));
            LOG.error(message);
            throw new RuntimeException(message, e);
        }
    }

    public Long getStartScn(Long startScn) {
        // 恢复位置不为0，则获取上一次读取的日志文件的起始位置开始读取
        if(null != startScn && startScn != 0L){
            startScn = getLogFileStartPositionByScn(startScn);
            return startScn;
        }

        // 恢复位置为0，则根据配置项进行处理
        if(ReadPosition.ALL.name().equalsIgnoreCase(logMinerConfig.getReadPosition())){
            // 获取最开始的scn
            startScn = getMinScn();
        } else if(ReadPosition.CURRENT.name().equalsIgnoreCase(logMinerConfig.getReadPosition())){
            startScn = getCurrentScn();
        } else if(ReadPosition.TIME.name().equalsIgnoreCase(logMinerConfig.getReadPosition())){
            // 根据指定的时间获取对应时间段的日志文件的起始位置
            if (logMinerConfig.getStartTime() == 0) {
                throw new IllegalArgumentException("[startTime] must not be null or empty when readMode is [time]");
            }

            startScn = getLogFileStartPositionByTime(logMinerConfig.getStartTime());
        } else  if(ReadPosition.SCN.name().equalsIgnoreCase(logMinerConfig.getReadPosition())){
            // 根据指定的scn获取对应日志文件的起始位置
            if(StringUtils.isEmpty(logMinerConfig.getStartScn())){
                throw new IllegalArgumentException("[startSCN] must not be null or empty when readMode is [scn]");
            }

            startScn = Long.parseLong(logMinerConfig.getStartScn());
        } else {
            throw new IllegalArgumentException("unsupported readMode : " + logMinerConfig.getReadPosition());
        }

        return startScn;
    }

    /**
     * oracle会把把重做日志分文件存储，每个文件都有 "FIRST_CHANGE" 和 "NEXT_CHANGE" 标识范围,
     * 这里需要根据给定scn找到对应的日志文件，并获取这个文件的 "FIRST_CHANGE"，然后从位置 "FIRST_CHANGE" 开始读取,
     * 在[FIRST_CHANGE,scn] 范围内的数据需要跳过。
     *
     * 视图说明：
     * v$archived_log 视图存储已经归档的日志文件
     * v$log 视图存储未归档的日志文件
     */
    private Long getLogFileStartPositionByScn(Long scn) {
        Long logFileFirstChange = null;
        PreparedStatement lastLogFileStmt = null;
        ResultSet lastLogFileResultSet = null;

        try {
            lastLogFileStmt = connection.prepareCall(isOracle10 ? SqlUtil.SQL_GET_LOG_FILE_START_POSITION_BY_SCN_10 : SqlUtil.SQL_GET_LOG_FILE_START_POSITION_BY_SCN);
            configStatement(lastLogFileStmt);

            lastLogFileStmt.setLong(1, scn);
            lastLogFileStmt.setLong(2, scn);
            lastLogFileResultSet = lastLogFileStmt.executeQuery();
            while(lastLogFileResultSet.next()){
                logFileFirstChange = lastLogFileResultSet.getLong(KEY_FIRST_CHANGE);
            }

            return logFileFirstChange;
        } catch (SQLException e) {
            LOG.error("根据scn:[{}]获取指定归档日志起始位置出错", scn, e);
            throw new RuntimeException(e);
        } finally {
            closeResources(lastLogFileResultSet, lastLogFileStmt, null);
        }
    }

    private Long getMinScn(){
        Long minScn = null;
        PreparedStatement minScnStmt = null;
        ResultSet minScnResultSet = null;

        try {
            minScnStmt = connection.prepareCall(SqlUtil.SQL_GET_LOG_FILE_START_POSITION);
            configStatement(minScnStmt);

            minScnResultSet = minScnStmt.executeQuery();
            while(minScnResultSet.next()){
                minScn = minScnResultSet.getLong(KEY_FIRST_CHANGE);
            }

            return minScn;
        } catch (SQLException e) {
            LOG.error("获取最早归档日志起始位置出错", e);
            throw new RuntimeException(e);
        } finally {
            closeResources(minScnResultSet, minScnStmt, null);
        }
    }

    private Long getCurrentScn() {
        Long currentScn = null;
        CallableStatement currentScnStmt = null;
        ResultSet currentScnResultSet = null;

        try {
            currentScnStmt = connection.prepareCall(SqlUtil.SQL_GET_CURRENT_SCN);
            configStatement(currentScnStmt);

            currentScnResultSet = currentScnStmt.executeQuery();
            while(currentScnResultSet.next()){
                currentScn = currentScnResultSet.getLong(KEY_CURRENT_SCN);
            }

            return currentScn;
        } catch (SQLException e) {
            LOG.error("获取当前的SCN出错:", e);
            throw new RuntimeException(e);
        } finally {
            closeResources(currentScnResultSet, currentScnStmt, null);
        }
    }

    private Long getLogFileStartPositionByTime(Long time) {
        Long logFileFirstChange = null;

        PreparedStatement lastLogFileStmt = null;
        ResultSet lastLogFileResultSet = null;

        try {
            String timeStr = DateFormatUtils.format(time, "yyyy-MM-dd HH:mm:ss");

            lastLogFileStmt = connection.prepareCall(isOracle10 ? SqlUtil.SQL_GET_LOG_FILE_START_POSITION_BY_TIME_10 : SqlUtil.SQL_GET_LOG_FILE_START_POSITION_BY_TIME);
            configStatement(lastLogFileStmt);

            lastLogFileStmt.setString(1, timeStr);
            lastLogFileStmt.setString(2, timeStr);

            if(!isOracle10){
                //oracle10只有两个参数
                lastLogFileStmt.setString(3, timeStr);
            }
            lastLogFileResultSet = lastLogFileStmt.executeQuery();
            while(lastLogFileResultSet.next()){
                logFileFirstChange = lastLogFileResultSet.getLong(KEY_FIRST_CHANGE);
            }

            return logFileFirstChange;
        } catch (SQLException e) {
            LOG.error("根据时间:[{}]获取指定归档日志起始位置出错", time, e);
            throw new RuntimeException(e);
        } finally {
            closeResources(lastLogFileResultSet, lastLogFileStmt, null);
        }
    }

    /**
     * 关闭数据库连接资源
     * @param rs
     * @param stmt
     * @param conn
     */
    private void closeResources(ResultSet rs, Statement stmt, Connection conn) {
        if (null != rs) {
            try {
                rs.close();
            } catch (SQLException e) {
                LOG.warn("Close resultSet error: {}", ExceptionUtil.getErrorMessage(e));
            }
        }

        closeStmt(stmt);

        if (null != conn) {
            try {
                conn.close();
            } catch (SQLException e) {
                LOG.warn("Close connection error:{}", ExceptionUtil.getErrorMessage(e));
            }
        }
    }

    /**
     * 根据scn号查询在线及归档日志组
     * @param scn
     * @return
     * @throws SQLException
     */
    private List<LogFile> queryLogFiles(Long scn) throws SQLException{
        List<LogFile> logFiles = new ArrayList<>();
        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            statement = connection.prepareStatement(isOracle10 ? SqlUtil.SQL_QUERY_LOG_FILE_10 : SqlUtil.SQL_QUERY_LOG_FILE);
            statement.setLong(1, scn);
            statement.setLong(2, scn);
            rs = statement.executeQuery();
            while (rs.next()) {
                LogFile logFile = new LogFile();
                logFile.setFileName(rs.getString("name"));
                logFile.setFirstChange(rs.getLong("first_change#"));
                logFile.setNextChange(rs.getLong("next_change#"));

                logFiles.add(logFile);
            }
        } finally {
            if (null != rs) {
                rs.close();
            }

            if (null != statement) {
                statement.close();
            }
        }

        lastQueryTime = System.currentTimeMillis();
        return logFiles;
    }

    public boolean hasNext() throws SQLException, UnsupportedEncodingException, DecoderException {
        if (null == logMinerData || logMinerData.isClosed()) {
            return false;
        }

        String sqlLog;
        while (logMinerData.next()) {
            String sql = logMinerData.getString(KEY_SQL_REDO);
            if(StringUtils.isBlank(sql)){
                continue;
            }
            StringBuilder sqlRedo = new StringBuilder(sql);
            if(SqlUtil.isCreateTemporaryTableSql(sqlRedo.toString())){
                continue;
            }
            long scn = logMinerData.getLong(KEY_SCN);
            String operation = logMinerData.getString(KEY_OPERATION);

            // 用CSF来判断一条sql是在当前这一行结束，sql超过4000 字节，会处理成多行
            boolean isSqlNotEnd = logMinerData.getBoolean(KEY_CSF);
            //是否存在多条SQL
            boolean hasMultiSql = isSqlNotEnd;

            while(isSqlNotEnd){
                logMinerData.next();
                sqlRedo.append(logMinerData.getString(KEY_SQL_REDO));
                isSqlNotEnd = logMinerData.getBoolean(KEY_CSF);
            }

            //oracle10中文编码且字符串大于4000，LogMiner可能出现中文乱码导致SQL解析异常
            if(hasMultiSql && isOracle10 && isGBK){
                String redo = sqlRedo.toString();

                String hexStr = new String(Hex.encodeHex(redo.getBytes("GBK")));

                if(hexStr.contains("3f2c") || hexStr.contains("3f20616e64")){
                    LOG.info("current scn is: {},\noriginal redo sql is: {},\nhex redo string is: {}", scn, redo, hexStr);
                    if("INSERT".equalsIgnoreCase(operation)){
                        //insert into values('','','',) value后可能存在中文乱码
                        //?, -> ',
                        hexStr = hexStr.replace("3f2c", "272c");
                    }else{
                        //update set "" = '' and "" = '' where "" = '' and "" = '' where后可能存在中文乱码
                        //delete from where "" = '' and "" = '' where后可能存在中文乱码
                        //?空格and -> '空格and
                        hexStr = hexStr.replace("3f20616e64", "2720616e64");
                    }
                    sqlLog = new String(Hex.decodeHex(hexStr.toCharArray()), "GBK");
                    LOG.info("final redo sql is: {}", sqlLog);
                }else{
                    sqlLog = new String(Hex.decodeHex(hexStr.toCharArray()), "GBK");
                }

            }else{
                sqlLog = sqlRedo.toString();
            }

            String schema = logMinerData.getString(KEY_SEG_OWNER);
            String tableName = logMinerData.getString(KEY_TABLE_NAME);
            Timestamp timestamp = logMinerData.getTimestamp(KEY_TIMESTAMP);

            Map<String, Object> data = new HashMap<>();
            data.put("schema", schema);
            data.put("tableName", tableName);
            data.put("operation", operation);
            data.put("sqlLog", sqlLog);
            data.put("opTime", timestamp);

            result = new QueueData(scn, data);
            return true;
        }

        return false;
    }

    //判断连接是否正常
    public boolean isValid()  {
        try {
            return connection != null && connection.isValid(2000);
        } catch (SQLException e) {
            return false;
        }
    }

    public void checkPrivileges() {
        try (Statement statement = connection.createStatement()) {

            queryDataBaseEncoding();

            List<String> roles = getUserRoles(statement);
            if (roles.contains(DBA_ROLE)) {
                return;
            }

            if (!roles.contains(EXECUTE_CATALOG_ROLE)) {
                throw new IllegalArgumentException("非DBA角色的用户必须是[EXECUTE_CATALOG_ROLE]角色,请执行sql赋权：GRANT EXECUTE_CATALOG_ROLE TO USERNAME");
            }

            if (containsNeededPrivileges(statement)) {
                return;
            }

            String message;
            if(ORACLE_11_VERSION <= oracleVersion){
                message = "权限不足，请执行sql赋权：GRANT CREATE SESSION, EXECUTE_CATALOG_ROLE, SELECT ANY TRANSACTION, FLASHBACK ANY TABLE, SELECT ANY TABLE, LOCK ANY TABLE, SELECT ANY DICTIONARY TO USER_ROLE;";
            }else{
                message = "权限不足，请执行sql赋权：GRANT LOGMINING, CREATE SESSION, SELECT ANY TRANSACTION ,SELECT ANY DICTIONARY TO USER_ROLE;";
            }

            throw new IllegalArgumentException(message);
        } catch (SQLException e) {
            throw new RuntimeException("检查权限出错", e);
        }
    }

    private boolean containsNeededPrivileges(Statement statement) {
        try (ResultSet rs = statement.executeQuery(SqlUtil.SQL_QUERY_PRIVILEGES)) {
            List<String> privileges = new ArrayList<>();
            while (rs.next()) {
                String privilege = rs.getString(KEY_PRIVILEGE);
                if (StringUtils.isNotEmpty(privilege)) {
                    privileges.add(privilege.toUpperCase());
                }
            }

            int privilegeCount = 0;
            List<String> privilegeList;
            if (oracleVersion <= ORACLE_11_VERSION) {
                privilegeList = ORACLE_11_PRIVILEGES_NEEDED;
            } else {
                privilegeList = PRIVILEGES_NEEDED;
            }
            for (String privilege : privilegeList) {
                if (privileges.contains(privilege)) {
                    privilegeCount++;
                }
            }

            return privilegeCount == privilegeList.size();
        } catch (SQLException e) {
            throw new RuntimeException("检查用户权限出错", e);
        }
    }

    private List<String> getUserRoles(Statement statement) {
        try (ResultSet rs = statement.executeQuery(SqlUtil.SQL_QUERY_ROLES)) {
            List<String> roles = new ArrayList<>();
            while (rs.next()) {
                String role = rs.getString(KEY_GRANTED_ROLE);
                if (StringUtils.isNotEmpty(role)) {
                    roles.add(role.toUpperCase());
                }
            }

            return roles;
        } catch (SQLException e) {
            throw new RuntimeException("检查用户角色出错", e);
        }
    }

    /**
     * 查询Oracle10数据库的字符编码
     */
    private void queryDataBaseEncoding(){
        if(isOracle10){
            try (Statement statement = connection.createStatement();
                 ResultSet rs = statement.executeQuery(SqlUtil.SQL_QUERY_ENCODING)) {
                rs.next();
                String encoding = rs.getString(1);
                LOG.info("current oracle encoding is {}", encoding);
                isGBK = encoding.contains("GBK");
            } catch (SQLException e) {
                throw new RuntimeException("检查用户角色出错", e);
            }
        }
    }

    private void configStatement(java.sql.Statement statement) throws SQLException {
        if (logMinerConfig.getQueryTimeout() != null) {
            statement.setQueryTimeout(logMinerConfig.getQueryTimeout().intValue());
        }
    }

    public QueueData next() {
        return result;
    }


    /**
     * 关闭logMinerSelectStmt
     */
    public void closeStmt(){
        try {
            if(logMinerSelectStmt != null && !logMinerSelectStmt.isClosed()){
                logMinerSelectStmt.close();
            }
        }catch (SQLException e){
            LOG.warn("Close logMinerSelectStmt error", e);
        }
        logMinerSelectStmt = null;
    }

    /**
     * 关闭Statement
     */
    private void closeStmt(Statement statement){
        try {
            if(statement != null && !statement.isClosed()){
                statement.close();
            }
        }catch (SQLException e){
            LOG.warn("Close statement error", e);
        }
    }

    public void setPreScn(Long preScn) {
        this.preScn = preScn;
    }

    public enum ReadPosition{
        ALL, CURRENT, TIME, SCN
    }
}
