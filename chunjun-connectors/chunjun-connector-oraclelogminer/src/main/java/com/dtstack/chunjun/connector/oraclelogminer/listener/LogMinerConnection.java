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

package com.dtstack.chunjun.connector.oraclelogminer.listener;

import com.dtstack.chunjun.cdc.DdlRowDataBuilder;
import com.dtstack.chunjun.cdc.EventType;
import com.dtstack.chunjun.connector.oraclelogminer.config.LogMinerConfig;
import com.dtstack.chunjun.connector.oraclelogminer.entity.OracleInfo;
import com.dtstack.chunjun.connector.oraclelogminer.entity.QueueData;
import com.dtstack.chunjun.connector.oraclelogminer.entity.RecordLog;
import com.dtstack.chunjun.connector.oraclelogminer.util.SqlUtil;
import com.dtstack.chunjun.element.ColumnRowData;
import com.dtstack.chunjun.element.column.StringColumn;
import com.dtstack.chunjun.element.column.TimestampColumn;
import com.dtstack.chunjun.util.ClassUtil;
import com.dtstack.chunjun.util.ExceptionUtil;
import com.dtstack.chunjun.util.GsonUtil;
import com.dtstack.chunjun.util.RetryUtil;

import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
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
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

@Slf4j
public class LogMinerConnection {
    public static final String KEY_PRIVILEGE = "PRIVILEGE";
    public static final String KEY_GRANTED_ROLE = "GRANTED_ROLE";
    public static final String CDB_CONTAINER_ROOT = "CDB$ROOT";
    public static final String DBA_ROLE = "DBA";
    public static final String LOG_TYPE_ARCHIVED = "ARCHIVED";
    public static final String EXECUTE_CATALOG_ROLE = "EXECUTE_CATALOG_ROLE";
    public static final int ORACLE_11_VERSION = 11;
    public static final List<String> PRIVILEGES_NEEDED =
            Arrays.asList(
                    "CREATE SESSION",
                    "LOGMINING",
                    "SELECT ANY TRANSACTION",
                    "SELECT ANY DICTIONARY");
    public static final List<String> ORACLE_11_PRIVILEGES_NEEDED =
            Arrays.asList("CREATE SESSION", "SELECT ANY TRANSACTION", "SELECT ANY DICTIONARY");
    public static final int RETRY_TIMES = 3;
    public static final int SLEEP_TIME = 2000;
    public static final String KEY_SEG_OWNER = "SEG_OWNER";
    public static final String KEY_TABLE_NAME = "TABLE_NAME";
    public static final String KEY_OPERATION = "OPERATION";
    public static final String KEY_OPERATION_CODE = "OPERATION_CODE";
    public static final String KEY_TIMESTAMP = "TIMESTAMP";
    public static final String KEY_SQL_REDO = "SQL_REDO";
    public static final String KEY_SQL_UNDO = "SQL_UNDO";
    public static final String KEY_CSF = "CSF";
    public static final String KEY_SCN = "SCN";
    public static final String KEY_CURRENT_SCN = "CURRENT_SCN";
    public static final String KEY_FIRST_CHANGE = "FIRST_CHANGE#";
    public static final String KEY_ROLLBACK = "ROLLBACK";
    public static final String KEY_ROW_ID = "ROW_ID";
    public static final String KEY_XID_USN = "XIDUSN";
    public static final String KEY_XID_SLT = "XIDSLT";
    public static final String KEY_XID_SQN = "XIDSQN";
    private static final long QUERY_LOG_INTERVAL = 10000;
    /** 加载状态集合 * */
    private final Set<STATE> LOADING =
            Sets.newHashSet(
                    LogMinerConnection.STATE.FILEADDED,
                    LogMinerConnection.STATE.FILEADDING,
                    LogMinerConnection.STATE.LOADING);

    private final LogMinerConfig logMinerConfig;
    private final AtomicReference<STATE> CURRENT_STATE = new AtomicReference<>(STATE.INITIALIZE);
    private final TransactionManager transactionManager;
    /** oracle数据源信息 * */
    public OracleInfo oracleInfo;
    /** 加载到logminer里的日志文件中 最小的nextChange * */
    protected BigInteger startScn = null;
    /** 加载到logminer里的日志文件中 最小的nextChange * */
    protected BigInteger endScn = null;

    private Connection connection;
    private CallableStatement logMinerStartStmt;
    private PreparedStatement logMinerSelectStmt;
    private ResultSet logMinerData;
    private QueueData result;
    private List<LogFile> addedLogFiles = new ArrayList<>();
    private long lastQueryTime;
    /** 为delete类型的rollback语句查找对应的insert语句的connection */
    private LogMinerConnection queryDataForRollbackConnection;

    private Exception exception;

    public LogMinerConnection(
            LogMinerConfig logMinerConfig, TransactionManager transactionManager) {
        this.logMinerConfig = logMinerConfig;
        this.transactionManager = transactionManager;
    }

    /** 获取oracle的信息 */
    public static OracleInfo getOracleInfo(Connection connection) throws SQLException {
        OracleInfo oracleInfo = new OracleInfo();

        oracleInfo.setVersion(connection.getMetaData().getDatabaseMajorVersion());

        try (Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery(SqlUtil.SQL_QUERY_ENCODING)) {
            rs.next();
            oracleInfo.setEncoding(rs.getString(1));
        }

        // 目前只有19才会判断是否是cdb模式
        if (oracleInfo.getVersion() == 19) {
            try (Statement statement = connection.createStatement();
                    ResultSet rs = statement.executeQuery(SqlUtil.SQL_IS_CDB)) {
                rs.next();
                oracleInfo.setCdbMode(rs.getString(1).equalsIgnoreCase("YES"));
            }
        }

        try (Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery(SqlUtil.SQL_IS_RAC)) {
            rs.next();
            oracleInfo.setRacMode(rs.getString(1).equalsIgnoreCase("TRUE"));
        }

        log.info("oracle info {}", oracleInfo);
        return oracleInfo;
    }

    public void connect() {
        try {
            ClassUtil.forName(logMinerConfig.getDriverName(), getClass().getClassLoader());

            connection = getConnection();

            oracleInfo = getOracleInfo(connection);

            // 修改session级别的 NLS_DATE_FORMAT 值为 "YYYY-MM-DD HH24:MI:SS"，否则在解析日志时 redolog出现
            // TO_DATE('18-APR-21', 'DD-MON-RR')

            try (PreparedStatement preparedStatement =
                    connection.prepareStatement(SqlUtil.SQL_ALTER_NLS_SESSION_PARAMETERS)) {
                preparedStatement.setQueryTimeout(logMinerConfig.getQueryTimeout().intValue());
                preparedStatement.execute();
            }

            // cdb需要会话在CDB$ROOT里
            if (oracleInfo.isCdbMode()) {
                try (PreparedStatement preparedStatement =
                        connection.prepareStatement(
                                String.format(
                                        SqlUtil.SQL_ALTER_SESSION_CONTAINER, CDB_CONTAINER_ROOT))) {
                    preparedStatement.setQueryTimeout(logMinerConfig.getQueryTimeout().intValue());
                    preparedStatement.execute();
                }
            }

            log.info(
                    "get connection successfully, url:{}, username:{}, Oracle info：{}",
                    logMinerConfig.getJdbcUrl(),
                    logMinerConfig.getUsername(),
                    oracleInfo);
        } catch (Exception e) {
            String message =
                    String.format(
                            "get connection failed，url:[%s], username:[%s], e:%s",
                            logMinerConfig.getJdbcUrl(),
                            logMinerConfig.getUsername(),
                            ExceptionUtil.getErrorMessage(e));
            log.error(message);
            // 出现异常 需要关闭connection,保证connection 和 session日期配置 生命周期一致
            closeResources(null, null, connection);
            throw new RuntimeException(message, e);
        }
    }

    /** 关闭LogMiner资源 */
    public void disConnect() {
        this.CURRENT_STATE.set(STATE.INITIALIZE);
        // 清除日志文件组，下次LogMiner启动时重新加载日志文件
        addedLogFiles.clear();

        if (null != logMinerStartStmt) {
            try {
                logMinerStartStmt.execute(SqlUtil.SQL_STOP_LOG_MINER);
            } catch (SQLException e) {
                log.warn("close logMiner failed, e = {}", ExceptionUtil.getErrorMessage(e));
            }
        }

        closeStmt(logMinerStartStmt);
        closeResources(logMinerData, logMinerSelectStmt, connection);

        // queryDataForRollbackConnection 也需要关闭资源
        if (Objects.nonNull(queryDataForRollbackConnection)) {
            queryDataForRollbackConnection.disConnect();
        }
    }

    /** 启动LogMiner */
    public void startOrUpdateLogMiner(BigInteger startScn, BigInteger endScn) {

        String startSql;
        try {
            this.startScn = startScn;
            this.endScn = endScn;
            this.CURRENT_STATE.set(STATE.FILEADDING);

            checkAndResetConnection();

            // 防止没有数据更新的时候频繁查询数据库，限定查询的最小时间间隔 QUERY_LOG_INTERVAL
            if (lastQueryTime > 0) {
                long time = System.currentTimeMillis() - lastQueryTime;
                if (time < QUERY_LOG_INTERVAL) {
                    try {
                        Thread.sleep(QUERY_LOG_INTERVAL - time);
                    } catch (InterruptedException e) {
                        log.warn("", e);
                    }
                }
            }
            lastQueryTime = System.currentTimeMillis();

            if (logMinerConfig.isSupportAutoAddLog()) {
                startSql =
                        oracleInfo.isOracle10()
                                ? SqlUtil.SQL_START_LOG_MINER_AUTO_ADD_LOG_10
                                : SqlUtil.SQL_START_LOG_MINER_AUTO_ADD_LOG;
            } else {
                startSql = SqlUtil.SQL_START_LOGMINER;
            }

            resetLogminerStmt(startSql);
            if (logMinerConfig.isSupportAutoAddLog()) {
                logMinerStartStmt.setString(1, startScn.toString());
            } else {
                logMinerStartStmt.setString(1, startScn.toString());
                logMinerStartStmt.setString(2, endScn.toString());
            }

            logMinerStartStmt.execute();
            this.CURRENT_STATE.set(STATE.FILEADDED);
            // 查找出加载到logMiner里的日志文件
            this.addedLogFiles = queryAddedLogFiles();
            log.info(
                    "Log group changed, startScn = {},endScn = {} new log group = {}",
                    startScn,
                    endScn,
                    GsonUtil.GSON.toJson(this.addedLogFiles));
        } catch (Exception e) {
            this.CURRENT_STATE.set(STATE.FAILED);
            this.exception = e;
            throw new RuntimeException(e);
        }
    }

    /** 从LogMiner视图查询数据 */
    public boolean queryData(String logMinerSelectSql) {

        try {

            this.CURRENT_STATE.set(STATE.LOADING);
            checkAndResetConnection();

            closeStmt();
            logMinerSelectStmt =
                    connection.prepareStatement(
                            logMinerSelectSql,
                            ResultSet.TYPE_FORWARD_ONLY,
                            ResultSet.CONCUR_READ_ONLY);
            configStatement(logMinerSelectStmt);

            logMinerSelectStmt.setFetchSize(logMinerConfig.getFetchSize());
            logMinerSelectStmt.setString(1, startScn.toString());
            logMinerSelectStmt.setString(2, endScn.toString());
            long before = System.currentTimeMillis();

            logMinerData = logMinerSelectStmt.executeQuery();

            this.CURRENT_STATE.set(STATE.READABLE);
            long timeConsuming = (System.currentTimeMillis() - before) / 1000;
            log.info(
                    "query LogMiner data, startScn:{},endScn:{},timeConsuming {}",
                    startScn,
                    endScn,
                    timeConsuming);
            return true;
        } catch (Exception e) {
            this.CURRENT_STATE.set(STATE.FAILED);
            this.exception = e;
            String message =
                    String.format(
                            "query logMiner data failed, sql:[%s], e: %s",
                            logMinerSelectSql, ExceptionUtil.getErrorMessage(e));
            throw new RuntimeException(message, e);
        }
    }

    /** 根据rollback的信息 找出对应的dml语句 */
    public void queryDataForDeleteRollback(
            RecordLog recordLog,
            BigInteger startScn,
            BigInteger endScn,
            BigInteger earliestEndScn,
            String sql) {
        try {
            this.CURRENT_STATE.set(STATE.LOADING);
            closeStmt();
            logMinerSelectStmt =
                    connection.prepareStatement(
                            sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            configStatement(logMinerSelectStmt);

            logMinerSelectStmt.setFetchSize(logMinerConfig.getFetchSize());
            logMinerSelectStmt.setString(1, recordLog.getXidUsn());
            logMinerSelectStmt.setString(2, recordLog.getXidSlt());
            logMinerSelectStmt.setString(3, recordLog.getXidSqn());
            logMinerSelectStmt.setString(4, recordLog.getTableName());
            logMinerSelectStmt.setInt(5, 0);
            logMinerSelectStmt.setInt(6, 1);
            logMinerSelectStmt.setInt(7, 3);
            logMinerSelectStmt.setString(8, String.valueOf(startScn));
            logMinerSelectStmt.setString(9, String.valueOf(endScn));
            logMinerSelectStmt.setString(10, String.valueOf(earliestEndScn));

            long before = System.currentTimeMillis();
            logMinerData = logMinerSelectStmt.executeQuery();
            long timeConsuming = (System.currentTimeMillis() - before) / 1000;
            log.info(
                    "queryDataForDeleteRollback, startScn:{},endScn:{},timeConsuming {}",
                    startScn,
                    endScn,
                    timeConsuming);
            this.CURRENT_STATE.set(STATE.READABLE);
        } catch (SQLException e) {
            String message =
                    String.format(
                            "queryDataForRollback failed, sql:[%s], recordLog:[%s] e: %s",
                            sql, recordLog, ExceptionUtil.getErrorMessage(e));
            log.error(message);
            throw new RuntimeException(message, e);
        }
    }

    public BigInteger getStartScn(BigInteger startScn) {
        // restart from checkpoint
        if (null != startScn && startScn.compareTo(BigInteger.ZERO) != 0) {
            return startScn;
        }

        // 恢复位置为0，则根据配置项进行处理
        if (ReadPosition.ALL.name().equalsIgnoreCase(logMinerConfig.getReadPosition())) {
            // 获取最开始的scn
            startScn = getMinScn();
        } else if (ReadPosition.CURRENT.name().equalsIgnoreCase(logMinerConfig.getReadPosition())) {
            startScn = getCurrentScn();
        } else if (ReadPosition.TIME.name().equalsIgnoreCase(logMinerConfig.getReadPosition())) {
            // 根据指定的时间获取对应时间段的日志文件的起始位置
            if (logMinerConfig.getStartTime() == 0) {
                throw new IllegalArgumentException(
                        "[startTime] must not be null or empty when readMode is [time]");
            }

            startScn = getLogFileStartPositionByTime(logMinerConfig.getStartTime());
        } else if (ReadPosition.SCN.name().equalsIgnoreCase(logMinerConfig.getReadPosition())) {
            // 根据指定的scn获取对应日志文件的起始位置
            if (StringUtils.isEmpty(logMinerConfig.getStartScn())) {
                throw new IllegalArgumentException(
                        "[startSCN] must not be null or empty when readMode is [scn]");
            }

            startScn = new BigInteger(logMinerConfig.getStartScn());
        } else {
            throw new IllegalArgumentException(
                    "unsupported readMode : " + logMinerConfig.getReadPosition());
        }

        return startScn;
    }

    private BigInteger getMinScn() {
        BigInteger minScn = null;
        PreparedStatement minScnStmt = null;
        ResultSet minScnResultSet = null;

        try {
            minScnStmt = connection.prepareCall(SqlUtil.SQL_GET_LOG_FILE_START_POSITION);
            configStatement(minScnStmt);

            minScnResultSet = minScnStmt.executeQuery();
            while (minScnResultSet.next()) {
                minScn = new BigInteger(minScnResultSet.getString(KEY_FIRST_CHANGE));
            }

            return minScn;
        } catch (SQLException e) {
            log.error(" obtaining the starting position of the earliest archive log error", e);
            throw new RuntimeException(e);
        } finally {
            closeResources(minScnResultSet, minScnStmt, null);
        }
    }

    protected BigInteger getCurrentScn() {
        BigInteger currentScn = null;
        CallableStatement currentScnStmt = null;
        ResultSet currentScnResultSet = null;

        try {
            currentScnStmt = connection.prepareCall(SqlUtil.SQL_GET_CURRENT_SCN);
            configStatement(currentScnStmt);

            currentScnResultSet = currentScnStmt.executeQuery();
            while (currentScnResultSet.next()) {
                currentScn = new BigInteger(currentScnResultSet.getString(KEY_CURRENT_SCN));
            }

            return currentScn;
        } catch (SQLException e) {
            log.error("获取当前的SCN出错:", e);
            throw new RuntimeException(e);
        } finally {
            closeResources(currentScnResultSet, currentScnStmt, null);
        }
    }

    private BigInteger getLogFileStartPositionByTime(Long time) {
        BigInteger logFileFirstChange = null;

        PreparedStatement lastLogFileStmt = null;
        ResultSet lastLogFileResultSet = null;

        try {
            String timeStr = DateFormatUtils.format(time, "yyyy-MM-dd HH:mm:ss");

            lastLogFileStmt =
                    connection.prepareCall(
                            oracleInfo.isOracle10()
                                    ? SqlUtil.SQL_GET_LOG_FILE_START_POSITION_BY_TIME_10
                                    : SqlUtil.SQL_GET_LOG_FILE_START_POSITION_BY_TIME);
            configStatement(lastLogFileStmt);

            lastLogFileStmt.setString(1, timeStr);
            lastLogFileStmt.setString(2, timeStr);

            if (!oracleInfo.isOracle10()) {
                // oracle10只有两个参数
                lastLogFileStmt.setString(3, timeStr);
            }
            lastLogFileResultSet = lastLogFileStmt.executeQuery();
            while (lastLogFileResultSet.next()) {
                logFileFirstChange =
                        new BigInteger(lastLogFileResultSet.getString(KEY_FIRST_CHANGE));
            }

            return logFileFirstChange;
        } catch (SQLException e) {
            log.error("根据时间:[{}]获取指定归档日志起始位置出错", time, e);
            throw new RuntimeException(e);
        } finally {
            closeResources(lastLogFileResultSet, lastLogFileStmt, null);
        }
    }

    /** 关闭数据库连接资源 */
    private void closeResources(ResultSet rs, Statement stmt, Connection conn) {
        if (null != rs) {
            try {
                rs.close();
            } catch (SQLException e) {
                log.warn("Close resultSet error: {}", ExceptionUtil.getErrorMessage(e));
            }
        }

        closeStmt(stmt);

        if (null != conn) {
            try {
                conn.close();
            } catch (SQLException e) {
                log.warn("Close connection error:{}", ExceptionUtil.getErrorMessage(e));
            }
        }
    }

    /** 根据leftScn 以及加载的日志大小限制 获取可加载的scn范围 以及此范围对应的日志文件 */
    protected Pair<BigInteger, Boolean> getEndScn(BigInteger startScn, List<LogFile> logFiles)
            throws SQLException {
        return getEndScn(startScn, logFiles, true);
    }

    protected Pair<BigInteger, Boolean> getEndScn(
            BigInteger startScn, List<LogFile> logFiles, boolean addRedoLog) throws SQLException {

        List<LogFile> logFileLists = new ArrayList<>();
        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            checkAndResetConnection();
            String sql;
            if (addRedoLog) {
                sql =
                        oracleInfo.isOracle10()
                                ? SqlUtil.SQL_QUERY_LOG_FILE_10
                                : SqlUtil.SQL_QUERY_LOG_FILE;
            } else {
                sql =
                        oracleInfo.isOracle10()
                                ? SqlUtil.SQL_QUERY_ARCHIVE_LOG_FILE_10
                                : SqlUtil.SQL_QUERY_ARCHIVE_LOG_FILE;
            }
            statement = connection.prepareStatement(sql);
            statement.setString(1, startScn.toString());
            statement.setString(2, startScn.toString());
            rs = statement.executeQuery();
            while (rs.next()) {
                LogFile logFile = new LogFile();
                logFile.setFileName(rs.getString("name"));
                logFile.setFirstChange(new BigInteger(rs.getString("first_change#")));
                logFile.setNextChange(new BigInteger(rs.getString("next_change#")));
                logFile.setThread(rs.getLong("thread#"));
                logFile.setBytes(rs.getLong("BYTES"));
                logFile.setType(rs.getString("TYPE"));
                logFileLists.add(logFile);
            }
        } finally {
            closeResources(rs, statement, null);
        }

        Map<Long, List<LogFile>> map =
                logFileLists.stream().collect(Collectors.groupingBy(LogFile::getThread));

        // 对每一个thread的文件进行排序
        map.forEach(
                (k, v) ->
                        map.put(
                                k,
                                v.stream()
                                        .sorted(Comparator.comparing(LogFile::getFirstChange))
                                        .collect(Collectors.toList())));

        BigInteger endScn = startScn;
        boolean loadRedoLog = false;

        long fileSize = 0L;
        Collection<List<LogFile>> values = map.values();

        while (fileSize < logMinerConfig.getMaxLogFileSize()) {
            List<LogFile> tempList = new ArrayList<>(8);
            for (List<LogFile> logFileList : values) {
                for (LogFile logFile1 : logFileList) {
                    if (!logFiles.contains(logFile1)) {
                        // 每个thread组文件每次只添加第一个
                        tempList.add(logFile1);
                        break;
                    }
                }
            }
            // 如果为空 代表没有可以加载的日志文件 结束循环
            if (CollectionUtils.isEmpty(tempList)) {
                break;
            }
            // 找到最小的nextSCN
            BigInteger minNextScn =
                    tempList.stream()
                            .sorted(Comparator.comparing(LogFile::getNextChange))
                            .collect(Collectors.toList())
                            .get(0)
                            .getNextChange();

            for (LogFile logFile1 : tempList) {
                if (logFile1.getFirstChange().compareTo(minNextScn) < 0) {
                    logFiles.add(logFile1);
                    fileSize += logFile1.getBytes();
                    if (logFile1.isOnline()) {
                        loadRedoLog = true;
                    }
                }
            }
            endScn = minNextScn;
        }

        if (loadRedoLog) {
            // 解决logminer偶尔丢失数据问题，读取online日志的时候，需要将rightScn置为当前SCN
            endScn = getCurrentScn();
            logFiles = logFileLists;
        }

        if (CollectionUtils.isEmpty(logFiles)) {
            return Pair.of(null, loadRedoLog);
        }

        log.info(
                "getEndScn success,startScn:{},endScn:{}, addRedoLog:{}, loadRedoLog:{}",
                startScn,
                endScn,
                addRedoLog,
                loadRedoLog);
        return Pair.of(endScn, loadRedoLog);
    }

    /** 获取logminer加载的日志文件 */
    private List<LogFile> queryAddedLogFiles() throws SQLException {
        List<LogFile> logFileLists = new ArrayList<>();
        try (PreparedStatement statement =
                connection.prepareStatement(SqlUtil.SQL_QUERY_ADDED_LOG)) {
            statement.setQueryTimeout(logMinerConfig.getQueryTimeout().intValue());
            try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    LogFile logFile = new LogFile();
                    logFile.setFileName(rs.getString("filename"));
                    logFile.setFirstChange(new BigInteger(rs.getString("low_scn")));
                    logFile.setNextChange(new BigInteger(rs.getString("next_scn")));
                    logFile.setThread(rs.getLong("thread_id"));
                    logFile.setBytes(rs.getLong("filesize"));
                    logFile.setStatus(rs.getInt("status"));
                    logFile.setType(rs.getString("type"));
                    logFileLists.add(logFile);
                }
            }
        }
        return logFileLists;
    }

    public boolean hasNext() throws SQLException, UnsupportedEncodingException, DecoderException {
        return hasNext(null, null);
    }

    public boolean hasNext(BigInteger endScn, String endRowid)
            throws SQLException, UnsupportedEncodingException, DecoderException {
        if (null == logMinerData
                || logMinerData.isClosed()
                || this.CURRENT_STATE.get().equals(STATE.READEND)) {
            this.CURRENT_STATE.set(STATE.READEND);
            return false;
        }

        String sqlLog;
        while (logMinerData.next()) {
            String sql = logMinerData.getString(KEY_SQL_REDO);
            if (StringUtils.isBlank(sql)) {
                continue;
            }
            StringBuilder sqlRedo = new StringBuilder(sql);
            StringBuilder sqlUndo =
                    new StringBuilder(
                            Objects.nonNull(logMinerData.getString(KEY_SQL_UNDO))
                                    ? logMinerData.getString(KEY_SQL_UNDO)
                                    : "");
            if (SqlUtil.isCreateTemporaryTableSql(sqlRedo.toString())) {
                continue;
            }
            BigInteger scn = new BigInteger(logMinerData.getString(KEY_SCN));
            String operation = logMinerData.getString(KEY_OPERATION);
            int operationCode = logMinerData.getInt(KEY_OPERATION_CODE);
            String tableName = logMinerData.getString(KEY_TABLE_NAME);

            boolean hasMultiSql;

            String xidSqn = logMinerData.getString(KEY_XID_SQN);
            String xidUsn = logMinerData.getString(KEY_XID_USN);
            String xidSLt = logMinerData.getString(KEY_XID_SLT);
            String rowId = logMinerData.getString(KEY_ROW_ID);
            boolean rollback = logMinerData.getBoolean(KEY_ROLLBACK);

            // 操作类型为commit / rollback，清理缓存
            // refer to
            // https://docs.oracle.com/cd/B19306_01/server.102/b14237/dynviews_1154.htm#REFRN30132
            if (operationCode == 7 || operationCode == 36) {
                transactionManager.cleanCache(xidUsn, xidSLt, xidSqn);
                continue;
            }

            if (endScn != null && rowId != null) {
                if (scn.compareTo(endScn) > 0) {
                    return false;
                }
                if (scn.compareTo(endScn) == 0 && rowId.equals(endRowid)) {
                    return false;
                }
            }
            // 用CSF来判断一条sql是在当前这一行结束，sql超过4000 字节，会处理成多行
            boolean isSqlNotEnd = logMinerData.getBoolean(KEY_CSF);
            // 是否存在多条SQL
            hasMultiSql = isSqlNotEnd;

            while (isSqlNotEnd) {
                logMinerData.next();
                // redoLog 实际上不需要发生切割  但是sqlUndo发生了切割，导致redolog值为null
                String sqlRedoValue = logMinerData.getString(KEY_SQL_REDO);
                if (Objects.nonNull(sqlRedoValue)) {
                    sqlRedo.append(sqlRedoValue);
                }

                String sqlUndoValue = logMinerData.getString(KEY_SQL_UNDO);
                if (Objects.nonNull(sqlUndoValue)) {
                    sqlUndo.append(sqlUndoValue);
                }
                isSqlNotEnd = logMinerData.getBoolean(KEY_CSF);
            }

            if (operationCode == 5) {
                result =
                        new QueueData(
                                scn,
                                DdlRowDataBuilder.builder()
                                        .setDatabaseName(null)
                                        .setSchemaName(logMinerData.getString(KEY_SEG_OWNER))
                                        .setTableName(tableName)
                                        .setContent(sqlRedo.toString())
                                        .setType(EventType.UNKNOWN.name())
                                        .setLsn(String.valueOf(scn))
                                        .setLsnSequence("0")
                                        .build());
                return true;
            }

            // delete from "table"."ID" where ROWID = 'AAADcjAAFAAAABoAAC' delete语句需要rowid条件需要替换
            // update "schema"."table" set "ID" = '29' 缺少where条件
            if (rollback && (operationCode == 2 || operationCode == 3)) {
                StringBuilder undoLog = new StringBuilder(1024);

                // 从缓存里查找rollback对应的DML语句
                RecordLog recordLog =
                        transactionManager.queryUndoLogFromCache(xidUsn, xidSLt, xidSqn);

                if (Objects.isNull(recordLog)) {

                    Pair<BigInteger, String> earliestRollbackOperation =
                            transactionManager.getEarliestRollbackOperation(xidUsn, xidSLt, xidSqn);
                    BigInteger earliestScn = scn;
                    String earliestRowid = rowId;
                    if (earliestRollbackOperation != null) {
                        earliestScn = earliestRollbackOperation.getLeft();
                        earliestRowid = earliestRollbackOperation.getRight();
                    }

                    // 如果DML语句不在缓存 或者 和rollback不再同一个日志文件里 会递归从日志文件里查找
                    recordLog =
                            recursionQueryDataForRollback(
                                    new RecordLog(
                                            scn,
                                            "",
                                            "",
                                            xidUsn,
                                            xidSLt,
                                            xidSqn,
                                            rowId,
                                            logMinerData.getString(KEY_TABLE_NAME),
                                            false,
                                            operationCode),
                                    earliestScn,
                                    earliestRowid);
                }

                if (Objects.nonNull(recordLog)) {
                    RecordLog rollbackLog =
                            new RecordLog(
                                    scn,
                                    sqlUndo.toString(),
                                    sqlRedo.toString(),
                                    xidUsn,
                                    xidSLt,
                                    xidSqn,
                                    rowId,
                                    tableName,
                                    hasMultiSql,
                                    operationCode);
                    String rollbackSql = getRollbackSql(rollbackLog, recordLog);
                    undoLog.append(rollbackSql);
                    hasMultiSql = recordLog.isHasMultiSql();
                }

                if (undoLog.length() == 0) {
                    // 没有查找到对应的insert语句 会将delete where rowid=xxx 语句作为redoLog
                    log.warn("has not found undoLog for scn {}", scn);
                } else {
                    sqlRedo = undoLog;
                }
                log.debug(
                        "find rollback sql,scn is {},rowId is {},xisSqn is {}", scn, rowId, xidSqn);
            }

            // oracle10中文编码且字符串大于4000，LogMiner可能出现中文乱码导致SQL解析异常
            if (hasMultiSql && oracleInfo.isOracle10() && oracleInfo.isGbk()) {
                String redo = sqlRedo.toString();
                String hexStr = new String(Hex.encodeHex(redo.getBytes("GBK")));
                boolean hasChange = false;
                if (operationCode == 1 && hexStr.contains("3f2c")) {
                    log.info(
                            "current scn is: {},\noriginal redo sql is: {},\nhex redo string is: {}",
                            scn,
                            redo,
                            hexStr);
                    hasChange = true;
                    hexStr = hexStr.replace("3f2c", "272c");
                }
                if (operationCode != 1) {
                    if (hexStr.contains("3f20616e64")) {
                        log.info(
                                "current scn is: {},\noriginal redo sql is: {},\nhex redo string is: {}",
                                scn,
                                redo,
                                hexStr);
                        hasChange = true;
                        // update set "" = '' and "" = '' where "" = '' and "" = '' where后可能存在中文乱码
                        // delete from where "" = '' and "" = '' where后可能存在中文乱码
                        // ?空格and -> '空格and
                        hexStr = hexStr.replace("3f20616e64", "2720616e64");
                    }

                    if (hexStr.contains("3f207768657265")) {
                        log.info(
                                "current scn is: {},\noriginal redo sql is: {},\nhex redo string is: {}",
                                scn,
                                redo,
                                hexStr);
                        hasChange = true;
                        // ? where 改为 ' where
                        hexStr = hexStr.replace("3f207768657265", "27207768657265");
                    }
                }

                if (hasChange) {
                    sqlLog = new String(Hex.decodeHex(hexStr.toCharArray()), "GBK");
                    log.info("final redo sql is: {}", sqlLog);
                } else {
                    sqlLog = sqlRedo.toString();
                }
            } else {
                sqlLog = sqlRedo.toString();
            }

            String schema = logMinerData.getString(KEY_SEG_OWNER);
            Timestamp timestamp = logMinerData.getTimestamp(KEY_TIMESTAMP);

            ColumnRowData columnRowData = new ColumnRowData(5);
            columnRowData.addField(new StringColumn(schema));
            columnRowData.addHeader("schema");

            columnRowData.addField(new StringColumn(tableName));
            columnRowData.addHeader("tableName");

            columnRowData.addField(new StringColumn(operation));
            columnRowData.addHeader("operation");

            columnRowData.addField(new StringColumn(sqlLog));
            columnRowData.addHeader("sqlLog");

            columnRowData.addField(new TimestampColumn(timestamp));
            columnRowData.addHeader("opTime");

            result = new QueueData(scn, columnRowData);

            // 只有非回滚的insert update解析的数据放入缓存
            if (!rollback) {
                transactionManager.putCache(
                        new RecordLog(
                                scn,
                                sqlUndo.toString(),
                                sqlLog,
                                xidUsn,
                                xidSLt,
                                xidSqn,
                                rowId,
                                tableName,
                                hasMultiSql,
                                operationCode));
            }
            return true;
        }

        this.CURRENT_STATE.set(STATE.READEND);
        return false;
    }

    // 判断连接是否正常
    public boolean isValid() {
        try {
            return connection != null
                    && connection.isValid(logMinerConfig.getQueryTimeout().intValue());
        } catch (SQLException e) {
            return false;
        }
    }

    public void checkPrivileges() {
        try (Statement statement = connection.createStatement()) {

            List<String> roles = getUserRoles(statement);
            if (roles.contains(DBA_ROLE)) {
                return;
            }

            if (!roles.contains(EXECUTE_CATALOG_ROLE)) {
                throw new IllegalArgumentException(
                        "users in non DBA roles must be [EXECUTE_CATALOG_ROLE] Role, please execute SQL GRANT：GRANT EXECUTE_CATALOG_ROLE TO USERNAME");
            }

            if (containsNeededPrivileges(statement)) {
                return;
            }

            String message;
            if (oracleInfo.getVersion() <= ORACLE_11_VERSION) {
                message =
                        "Insufficient permissions, please execute sql authorization：GRANT CREATE SESSION, EXECUTE_CATALOG_ROLE, SELECT ANY TRANSACTION, FLASHBACK ANY TABLE, SELECT ANY TABLE, LOCK ANY TABLE, SELECT ANY DICTIONARY TO USER_ROLE;";
            } else {
                message =
                        "Insufficient permissions, please execute sql authorization：GRANT LOGMINING, CREATE SESSION, SELECT ANY TRANSACTION ,SELECT ANY DICTIONARY TO USER_ROLE;";
            }

            throw new IllegalArgumentException(message);
        } catch (SQLException e) {
            throw new RuntimeException("check permissions failed", e);
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
            if (oracleInfo.getVersion() <= ORACLE_11_VERSION) {
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
            throw new RuntimeException("check user permissions error", e);
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
            throw new RuntimeException("check user permissions error", e);
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

    /** 关闭logMinerSelectStmt */
    public void closeStmt() {
        try {
            if (logMinerSelectStmt != null && !logMinerSelectStmt.isClosed()) {
                logMinerSelectStmt.close();
            }
        } catch (SQLException e) {
            log.warn("Close logMinerSelectStmt error", e);
        }
        logMinerSelectStmt = null;
    }

    /** 关闭Statement */
    private void closeStmt(Statement statement) {
        try {
            if (statement != null && !statement.isClosed()) {
                statement.close();
            }
        } catch (SQLException e) {
            log.warn("Close statement error", e);
        }
    }

    /**
     * 递归查找 delete的rollback对应的insert语句
     *
     * @param rollbackRecord rollback参数
     * @return insert语句
     */
    public RecordLog recursionQueryDataForRollback(
            RecordLog rollbackRecord, BigInteger earliestEndScn, String earliestRowid)
            throws SQLException, UnsupportedEncodingException, DecoderException {
        if (Objects.isNull(queryDataForRollbackConnection)) {
            queryDataForRollbackConnection =
                    new LogMinerConnection(logMinerConfig, transactionManager);
        }

        if (Objects.isNull(queryDataForRollbackConnection.connection)
                || queryDataForRollbackConnection.connection.isClosed()) {
            log.info("queryDataForRollbackConnection start connect");
            queryDataForRollbackConnection.connect();
        }

        BigInteger endScn = earliestEndScn;
        BigInteger minScn = getMinScn();
        for (int i = 0; ; i++) {
            BigInteger startScn =
                    endScn.subtract(new BigInteger("5000"))
                            .subtract(new BigInteger((2000 * i) + ""));
            if (startScn.compareTo(minScn) <= 0) {
                startScn = minScn;
            }
            log.info(
                    "queryDataForRollbackConnection startScn{}, endScn{}, earliestEndScn:{}, rowid:{},table:{}",
                    startScn,
                    endScn,
                    earliestEndScn,
                    earliestRowid,
                    rollbackRecord.getTableName());
            queryDataForRollbackConnection.startOrUpdateLogMiner(
                    startScn, endScn.add(BigInteger.ONE));
            queryDataForRollbackConnection.queryDataForDeleteRollback(
                    rollbackRecord, startScn, endScn, earliestEndScn, SqlUtil.queryDataForRollback);
            // while循环查找所有数据 并都加载到缓存里去
            while (queryDataForRollbackConnection.hasNext(earliestEndScn, earliestRowid)) {}
            // 从缓存里取
            RecordLog dmlLog =
                    transactionManager.queryUndoLogFromCache(
                            rollbackRecord.getXidUsn(),
                            rollbackRecord.getXidSlt(),
                            rollbackRecord.getXidSqn());
            if (Objects.nonNull(dmlLog)) {
                return dmlLog;
            }
            endScn = startScn;
            if (startScn.compareTo(minScn) <= 0) {
                log.warn(
                        "select all file but not found log for rollback data, xidUsn {},xidSlt {},xidSqn {},scn {}",
                        rollbackRecord.getXidUsn(),
                        rollbackRecord.getXidSlt(),
                        rollbackRecord.getXidSqn(),
                        rollbackRecord.getScn());
                break;
            }
        }

        return null;
    }

    /** 重置 启动logminer的statement */
    public void resetLogminerStmt(String startSql) throws SQLException {
        closeStmt(logMinerStartStmt);
        logMinerStartStmt = connection.prepareCall(startSql);
        configStatement(logMinerStartStmt);
    }

    public void checkAndResetConnection() {
        if (!isValid()) {
            connect();
        }
    }

    /**
     * 回滚语句根据对应的dml日志找出对应的undoog
     *
     * @param rollbackLog 回滚语句
     * @param dmlLog 对应的dml语句
     */
    public String getRollbackSql(RecordLog rollbackLog, RecordLog dmlLog) {
        // 如果回滚日志是update，则其where条件没有 才会进入
        if (rollbackLog.getOperationCode() == 3 && dmlLog.getOperationCode() == 3) {
            return dmlLog.getSqlUndo();
        }

        // 回滚日志是delete
        // delete回滚两种场景 如果客户表字段存在blob字段且插入时blob字段为空 此时会出现insert emptyBlob语句以及一个update语句之外
        // 之后才会有一个delete语句，而此delete语句rowid对应的上面update语句 所以直接返回delete from "table"."ID" where ROWID =
        // 'AAADcjAAFAAAABoAAC'（blob不支持）
        if (rollbackLog.getOperationCode() == 2 && dmlLog.getOperationCode() == 1) {
            return dmlLog.getSqlUndo();
        }
        log.warn("dmlLog [{}]  is not hit for rollbackLog [{}]", dmlLog, rollbackLog);
        return "";
    }

    /** 是否处于加载状态 * */
    public boolean isLoading() {
        return LOADING.contains(this.CURRENT_STATE.get());
    }

    @Override
    public String toString() {
        return "LogMinerConnection{"
                + "startScn="
                + startScn
                + ",endScn="
                + endScn
                + ",currentState="
                + CURRENT_STATE
                + '}';
    }

    public STATE getState() {
        return this.CURRENT_STATE.get();
    }

    public enum ReadPosition {
        ALL,
        CURRENT,
        TIME,
        SCN
    }

    public enum STATE {
        INITIALIZE,
        FILEADDING,
        FILEADDED,
        LOADING,
        READABLE,
        READEND,
        FAILED
    }

    protected Connection getConnection() {
        java.util.Properties info = new java.util.Properties();
        if (logMinerConfig.getUsername() != null) {
            info.put("user", logMinerConfig.getUsername());
        }
        if (logMinerConfig.getPassword() != null) {
            info.put("password", logMinerConfig.getPassword());
        }

        // queryTimeOut 单位是秒 需要转为毫秒
        info.put("oracle.jdbc.ReadTimeout", (logMinerConfig.getQueryTimeout() + 60) * 1000 + "");

        if (Objects.nonNull(logMinerConfig.getProperties())) {
            info.putAll(logMinerConfig.getProperties());
        }
        Properties printProperties = new Properties();
        printProperties.putAll(info);
        printProperties.put("password", "******");
        log.info("connection properties is {}", printProperties);
        return RetryUtil.executeWithRetry(
                () -> DriverManager.getConnection(logMinerConfig.getJdbcUrl(), info),
                RETRY_TIMES,
                SLEEP_TIME,
                false);
    }

    public Exception getE() {
        return exception;
    }
}
