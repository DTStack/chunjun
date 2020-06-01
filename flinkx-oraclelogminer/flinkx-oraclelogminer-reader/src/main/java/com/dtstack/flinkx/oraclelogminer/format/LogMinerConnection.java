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
import com.dtstack.flinkx.util.RetryUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateFormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;
import java.util.concurrent.Callable;

/**
 * @author jiangbo
 * @date 2020/3/27
 */
public class LogMinerConnection {

    public static Logger LOG = LoggerFactory.getLogger(LogMinerConnection.class);

    public static final String KEY_PRIVILEGE = "PRIVILEGE";
    public static final String KEY_GRANTED_ROLE = "GRANTED_ROLE";

    public static final String DBA_ROLE = "DBA";
    public static final String EXECUTE_CATALOG_ROLE = "EXECUTE_CATALOG_ROLE";

    public static final List<String> PRIVILEGES_NEEDED = Arrays.asList(
            "CREATE SESSION",
            "LOGMINING",
            "SELECT ANY TRANSACTION",
            "SELECT ANY DICTIONARY");

    private static final int RETRY_TIMES = 3;

    private static final int SLEEP_TIME = 2000;

    public final static String KEY_SEG_OWNER = "SEG_OWNER";
    public final static String KEY_TABLE_NAME = "TABLE_NAME";
    public final static String KEY_OPERATION = "OPERATION";
    public final static String KEY_SQL_REDO = "SQL_REDO";
    public final static String KEY_CSF = "CSF";
    public final static String KEY_SCN = "SCN";
    public final static String KEY_CURRENT_SCN = "CURRENT_SCN";
    public final static String KEY_FIRST_CHANGE = "FIRST_CHANGE#";

    private LogMinerConfig logMinerConfig;

    private Connection connection;

    private CallableStatement logMinerStartStmt;

    private PreparedStatement logMinerSelectStmt;

    private ResultSet logMinerData;

    private QueueData result;

    private List<LogFile> addedLogFiles = new ArrayList<>();

    private long lastQueryTime;

    private static final long QUERY_LOG_INTERVAL = 1000;

    private boolean logMinerStarted = false;

    public LogMinerConnection(LogMinerConfig logMinerConfig) {
        this.logMinerConfig = logMinerConfig;
    }

    public void connect() {
        try {
            ClassUtil.forName(logMinerConfig.getDriverName(), getClass().getClassLoader());

            connection = RetryUtil.executeWithRetry(new Callable<Connection>() {
                @Override
                public Connection call() throws Exception {
                    return DriverManager.getConnection(logMinerConfig.getJdbcUrl(), logMinerConfig.getUsername(), logMinerConfig.getPassword());
                }
            }, RETRY_TIMES, SLEEP_TIME,false);

            LOG.info("获取连接成功,url:{}, username:{}", logMinerConfig.getJdbcUrl(), logMinerConfig.getUsername());
        } catch (Exception e){
            LOG.error("获取连接失败，url:{}, username:{}", logMinerConfig.getJdbcUrl(), logMinerConfig.getUsername());
            throw new RuntimeException(e);
        }
    }

    public void disConnect() throws SQLException{
        if (null != logMinerStartStmt && logMinerStarted) {
            logMinerStartStmt.execute(SqlUtil.SQL_STOP_LOG_MINER);
            logMinerStarted = false;
        }

        if (null != logMinerData) {
            logMinerData.close();
        }

        if (null != logMinerStartStmt) {
            logMinerStartStmt.close();
        }

        if (null != logMinerSelectStmt) {
            logMinerSelectStmt.close();
        }

        if (null != connection) {
            connection.close();
        }
    }

    public void startOrUpdateLogMiner(Long startScn) {
        String startSql = null;
        try {
            if (logMinerConfig.getSupportAutoAddLog()) {
                startSql = SqlUtil.SQL_START_LOG_MINER_AUTO_ADD_LOG;
            } else {
                List<LogFile> newLogFiles = queryLogFiles(startScn);
                if (addedLogFiles.equals(newLogFiles)) {
                    return;
                } else {
                    addedLogFiles = newLogFiles;
                    startSql = SqlUtil.SQL_START_LOG_MINER;
                }
            }

            logMinerStartStmt = connection.prepareCall(startSql);
            configStatement(logMinerStartStmt);

            logMinerStartStmt.setLong(1, startScn);
            logMinerStartStmt.execute();

            logMinerStarted = true;
            LOG.info("启动Log miner成功,offset:{}， sql:{}", startScn, startSql);
        } catch (SQLException e){
            LOG.error("启动Log miner失败,offset:{}， sql:{}", startScn, startSql);
            throw new RuntimeException(e);
        }
    }

    public void queryData(Long startScn) {
        String logMinerSelectSql = SqlUtil.buildSelectSql(logMinerConfig.getCat(), logMinerConfig.getListenerTables());
        try {
            logMinerSelectStmt = connection.prepareStatement(logMinerSelectSql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            configStatement(logMinerSelectStmt);

            logMinerSelectStmt.setFetchSize(logMinerConfig.getFetchSize());
            logMinerSelectStmt.setLong(1, startScn);
            logMinerData = logMinerSelectStmt.executeQuery();

            LOG.info("查询Log miner数据,sql:{}, offset:{}", logMinerSelectSql, startScn);
        } catch (SQLException e) {
            LOG.error("查询Log miner数据出错,sql:{}", logMinerSelectSql);
            throw new RuntimeException(e);
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
                throw new RuntimeException("读取模式为[time]时必须指定[startTime]");
            }

            startScn = getLogFileStartPositionByTime(logMinerConfig.getStartTime());
        } else  if(ReadPosition.SCN.name().equalsIgnoreCase(logMinerConfig.getReadPosition())){
            // 根据指定的scn获取对应日志文件的起始位置
            if(StringUtils.isEmpty(logMinerConfig.getStartSCN())){
                throw new RuntimeException("读取模式为[scn]时必须指定[startSCN]");
            }

            startScn = Long.parseLong(logMinerConfig.getStartSCN());
        } else {
            throw new RuntimeException("不支持的读取模式:" + logMinerConfig.getReadPosition());
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
            lastLogFileStmt = connection.prepareCall(SqlUtil.SQL_GET_LOG_FILE_START_POSITION_BY_SCN);
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

            lastLogFileStmt = connection.prepareCall(SqlUtil.SQL_GET_LOG_FILE_START_POSITION_BY_TIME);
            configStatement(lastLogFileStmt);

            lastLogFileStmt.setString(1, timeStr);
            lastLogFileStmt.setString(2, timeStr);
            lastLogFileStmt.setString(3, timeStr);
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

    private void closeResources(ResultSet rs, Statement stmt, Connection conn) {
        if (null != rs) {
            try {
                rs.close();
            } catch (SQLException e) {
                LOG.warn("Close resultSet error: {}", ExceptionUtil.getErrorMessage(e));
                throw new RuntimeException(e);
            }
        }

        if (null != stmt) {
            try {
                stmt.close();
            } catch (SQLException e) {
                LOG.warn("Close statement error:{}", ExceptionUtil.getErrorMessage(e));
                throw new RuntimeException(e);
            }
        }

        if (null != conn) {
            try {
                conn.close();
            } catch (SQLException e) {
                LOG.warn("Close connection error:{}", ExceptionUtil.getErrorMessage(e));
                throw new RuntimeException(e);
            }
        }
    }

    private List<LogFile> queryLogFiles(Long scn) throws SQLException{
        // 防止没有数据更新的时候频繁查询数据库，限定查询的最小时间间隔 QUERY_LOG_INTERVAL
        if (lastQueryTime > 0) {
            long time = System.currentTimeMillis() - lastQueryTime;
            if (time < QUERY_LOG_INTERVAL) {
                try {
                    Thread.sleep(QUERY_LOG_INTERVAL);
                } catch (InterruptedException e) {
                    LOG.warn("", e);
                }
            }
        }

        List<LogFile> logFiles = new ArrayList<>();
        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            statement = connection.prepareStatement(SqlUtil.SQL_QUERY_LOG_FILE);
            statement.setLong(1, scn);
            statement.setLong(2, scn);
            rs = statement.executeQuery();
            while (rs.next()) {
                LogFile logFile = new LogFile();
                logFile.setFileName(rs.getString("name"));
                logFile.setFirstChange(rs.getLong("first_change#"));

                String nextChangeString = rs.getString("next_change#");
                if (nextChangeString.length() == 20) {
                    logFile.setNextChange(Long.MAX_VALUE);
                } else {
                    logFile.setNextChange(Long.parseLong(nextChangeString));
                }

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

    public boolean hasNext() throws SQLException{
        if (null == logMinerData) {
            return false;
        }

        String sqlLog;
        while (logMinerData.next()) {
            long scn = logMinerData.getLong(KEY_SCN);

            // 用CSF来判断一条sql是在当前这一行结束，sql超过4000 字节，会处理成多行
            boolean isSqlNotEnd = logMinerData.getBoolean(KEY_CSF);

            StringBuilder sqlRedo = new StringBuilder(logMinerData.getString(KEY_SQL_REDO));
            if(SqlUtil.isCreateTemporaryTableSql(sqlRedo.toString())){
                continue;
            }

            while(isSqlNotEnd){
                logMinerData.next();
                sqlRedo.append(logMinerData.getString(KEY_SQL_REDO));
                isSqlNotEnd = logMinerData.getBoolean(KEY_CSF);
            }

            sqlLog = sqlRedo.toString();
            String schema = logMinerData.getString(KEY_SEG_OWNER);
            String tableName = logMinerData.getString(KEY_TABLE_NAME);
            String operation = logMinerData.getString(KEY_OPERATION);

            Map<String, Object> data = new HashMap<>();
            data.put("schema", schema);
            data.put("tableName", tableName);
            data.put("operation", operation);
            data.put("sqlLog", sqlLog);

            result = new QueueData(scn, data);
            return true;
        }

        return false;
    }

    public void checkPrivileges() {
        try (Statement statement = connection.createStatement()) {
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

            throw new IllegalArgumentException("权限不足，请执行sql赋权：GRANT LOGMINING, CREATE SESSION, SELECT ANY TRANSACTION ,SELECT ANY DICTIONARY TO USER_ROLE;");
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
            for (String privilege : PRIVILEGES_NEEDED) {
                if (privileges.contains(privilege)) {
                    privilegeCount++;
                }
            }

            return privilegeCount == 4;
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

    private void configStatement(java.sql.Statement statement) throws SQLException {
        if (logMinerConfig.getQueryTimeout() != null) {
            statement.setQueryTimeout(logMinerConfig.getQueryTimeout().intValue());
        }
    }

    public QueueData next() {
        return result;
    }

    enum ReadPosition{
        ALL, CURRENT, TIME, SCN
    }
}
