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

import com.dtstack.flinkx.inputformat.RichInputFormat;
import com.dtstack.flinkx.oraclelogminer.util.LogMinerUtil;
import com.dtstack.flinkx.restore.FormatState;
import com.dtstack.flinkx.util.ClassUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.io.Serializable;
import java.sql.*;
import java.text.SimpleDateFormat;

/**
 * @author jiangbo
 * @date 2019/12/14
 *
 * 名词说明:
 * SCN 即系统改变号(System Change Number)
 */
public class OracleLogMinerInputFormat extends RichInputFormat {

    public LogMinerConfig logMinerConfig;

    private transient Connection connection;

    private transient CallableStatement logMinerStartStmt;

    private transient PreparedStatement logMinerSelectStmt;

    private transient ResultSet logMinerData;

    private Long offsetScn;

    private Long scnCopy;

    private boolean skipRecord = true;

    @Override
    public void configure(Configuration parameters) {
        // do nothing
    }

    @Override
    public void openInputFormat() throws IOException {
        super.openInputFormat();

        try {
            ClassUtil.forName(logMinerConfig.getDriverName(), getClass().getClassLoader());
            connection = DriverManager.getConnection(logMinerConfig.getJdbcUrl(), logMinerConfig.getUsername(), logMinerConfig.getPassword());

            LOG.info("获取连接成功,url:{}, username:{}", logMinerConfig.getJdbcUrl(), logMinerConfig.getUsername());
        } catch (SQLException e){
            LOG.error("获取连接失败，url:{}, username:{}", logMinerConfig.getJdbcUrl(), logMinerConfig.getUsername());
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void openInternal(InputSplit inputSplit) throws IOException {
        initOffset();

        System.out.println("offset:" + offsetScn);
        System.out.println("scnCopy:" + scnCopy);

        startLogMiner();
        startSelectData();
    }

    private void initOffset(){
        if(formatState != null && formatState.getState() != null){
            offsetScn = Long.parseLong(formatState.getState().toString());
        } else {
            offsetScn = 0L;
        }

        // 恢复位置不为0，则获取上一次读取的日志文件的起始位置开始读取
        if(offsetScn != 0L){
            scnCopy = offsetScn;
            offsetScn = getLogFileStartPositionByScn(scnCopy);
            return;
        }

        // 恢复位置为0，则根据配置项进行处理
        if(ReadPosition.ALL.name().equalsIgnoreCase(logMinerConfig.getReadPosition())){
            skipRecord = false;
            // 获取最开始的scn
            offsetScn = getMinScn();
        } else if(ReadPosition.CURRENT.name().equalsIgnoreCase(logMinerConfig.getReadPosition())){
            // FIXME 获取到的 scn 比实际的值要大
            scnCopy = getCurrentScn();
            offsetScn = getLogFileStartPositionByScn(scnCopy);
        } else if(ReadPosition.TIME.name().equalsIgnoreCase(logMinerConfig.getReadPosition())){
            skipRecord = false;

            // 根据指定的时间获取对应时间段的日志文件的起始位置
            if (logMinerConfig.getStartTime() == 0) {
                throw new RuntimeException("读取模式为[time]时必须指定[startTime]");
            }

            offsetScn = getLogFileStartPositionByTime(logMinerConfig.getStartTime());
        } else  if(ReadPosition.SCN.name().equalsIgnoreCase(logMinerConfig.getReadPosition())){
            scnCopy = Long.parseLong(logMinerConfig.getStartSCN());

            // 根据指定的scn获取对应日志文件的起始位置
            if(StringUtils.isEmpty(logMinerConfig.getStartSCN())){
                throw new RuntimeException("读取模式为[scn]时必须指定[startSCN]");
            }

            offsetScn = getLogFileStartPositionByScn(Long.parseLong(logMinerConfig.getStartSCN()));
        } else {
            throw new RuntimeException("不支持的读取模式:" + logMinerConfig.getReadPosition());
        }
    }

    private Long getMinScn(){
        Long minScn = null;
        try {
            PreparedStatement minScnStmt = connection.prepareCall(LogMinerUtil.SQL_GET_LOG_FILE_START_POSITION);
            ResultSet minScnResultSet = minScnStmt.executeQuery();
            while(minScnResultSet.next()){
                minScn = minScnResultSet.getLong(LogMinerUtil.KEY_FIRST_CHANGE);
            }

            minScnStmt.close();
            minScnResultSet.close();

            return minScn;
        } catch (SQLException e) {
            LOG.error("获取最早归档日志起始位置出错", e);
            throw new RuntimeException(e);
        }
    }

    private Long getCurrentScn() {
        Long currentScn = null;
        try {
            CallableStatement currentScnStmt = connection.prepareCall(LogMinerUtil.SQL_GET_CURRENT_SCN);
            ResultSet currentScnResultSet = currentScnStmt.executeQuery();
            while(currentScnResultSet.next()){
                currentScn = currentScnResultSet.getLong(LogMinerUtil.KEY_CURRENT_SCN);
            }

            currentScnResultSet.close();
            currentScnStmt.close();

            return currentScn;
        } catch (SQLException e) {
            LOG.error("获取当前的SCN出错:", e);
            throw new RuntimeException(e);
        }
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

        try {
            PreparedStatement lastLogFileStmt = connection.prepareCall(LogMinerUtil.SQL_GET_LOG_FILE_START_POSITION_BY_SCN);
            lastLogFileStmt.setLong(1, scn);
            lastLogFileStmt.setLong(2, scn);
            ResultSet lastLogFileResultSet = lastLogFileStmt.executeQuery();
            while(lastLogFileResultSet.next()){
                logFileFirstChange = lastLogFileResultSet.getLong(LogMinerUtil.KEY_FIRST_CHANGE);
            }

            lastLogFileStmt.close();
            lastLogFileResultSet.close();

            return logFileFirstChange;
        } catch (SQLException e) {
            LOG.error("根据scn:[{}]获取指定归档日志起始位置出错", scn, e);
            throw new RuntimeException(e);
        }
    }

    private Long getLogFileStartPositionByTime(Long time) {
        Long logFileFirstChange = null;

        try {
            String timeStr = DateFormatUtils.format(time, "yyyy-MM-dd HH:mm:ss");

            PreparedStatement lastLogFileStmt = connection.prepareCall(LogMinerUtil.SQL_GET_LOG_FILE_START_POSITION_BY_TIME);
            lastLogFileStmt.setString(1, timeStr);
            lastLogFileStmt.setString(2, timeStr);
            lastLogFileStmt.setString(3, timeStr);
            ResultSet lastLogFileResultSet = lastLogFileStmt.executeQuery();
            while(lastLogFileResultSet.next()){
                logFileFirstChange = lastLogFileResultSet.getLong(LogMinerUtil.KEY_FIRST_CHANGE);
            }

            lastLogFileStmt.close();
            lastLogFileResultSet.close();

            return logFileFirstChange;
        } catch (SQLException e) {
            LOG.error("根据时间:[{}]获取指定归档日志起始位置出错", time, e);
            throw new RuntimeException(e);
        }
    }

    private void startLogMiner(){
        try {
            logMinerStartStmt = connection.prepareCall(LogMinerUtil.SQL_START_LOGMINER);
            logMinerStartStmt.setLong(1, offsetScn);
            logMinerStartStmt.execute();

            LOG.info("启动Log miner成功,offset:{}， sql:{}", offsetScn, LogMinerUtil.SQL_START_LOGMINER);
        } catch (SQLException e){
            LOG.error("启动Log miner失败,offset:{}， sql:{}", offsetScn, LogMinerUtil.SQL_START_LOGMINER);
            throw new RuntimeException(e);
        }
    }

    private void startSelectData() {
        String logMinerSelectSql = LogMinerUtil.buildSelectSql(logMinerConfig.getListenerOperations(), logMinerConfig.getListenerTables());
        try {
            logMinerSelectStmt = connection.prepareStatement(logMinerSelectSql);
            logMinerSelectStmt.setFetchSize(logMinerConfig.getFetchSize());
            logMinerSelectStmt.setLong(1, offsetScn);
            logMinerData = logMinerSelectStmt.executeQuery();

            LOG.info("查询Log miner数据,sql:{}, offset:{}", logMinerSelectSql, offsetScn);
        } catch (SQLException e) {
            LOG.error("查询Log miner数据出错,sql:{}", logMinerSelectSql);
            throw new RuntimeException(e);
        }
    }

    @Override
    protected Row nextRecordInternal(Row row) throws IOException {
        try {
            while (logMinerData.next()) {
                Long scn = logMinerData.getLong(LogMinerUtil.KEY_SCN);

                // 用CSF来判断一条sql是在当前这一行结束，sql超过4000 字节，会处理成多行
                boolean isSqlNotEnd = logMinerData.getBoolean(LogMinerUtil.KEY_CSF);

                if (skipRecord){
                    if (scn > scnCopy && !isSqlNotEnd){
                        skipRecord = false;
                        continue;
                    }

                    LOG.debug("Skipping data with scn :{}", scn);
                    continue;
                }

                StringBuilder sqlRedo = new StringBuilder(logMinerData.getString(LogMinerUtil.KEY_SQL_REDO));
                if(LogMinerUtil.isCreateTemporaryTableSql(sqlRedo.toString())){
                    continue;
                }

                while(isSqlNotEnd){
                    logMinerData.next();
                    sqlRedo.append(logMinerData.getString(LogMinerUtil.KEY_SQL_REDO));
                    isSqlNotEnd = logMinerData.getBoolean(LogMinerUtil.KEY_CSF);
                }

                row = LogMinerUtil.parseSql(logMinerData, sqlRedo.toString(), logMinerConfig.getPavingData());

                offsetScn = scn;
                return row;
            }
        } catch (Exception e) {
            LOG.error("解析数据出错:", e);
            throw new RuntimeException(e);
        }

        throw new RuntimeException("获取不到下一条数据，程序自动失败");
    }

    @Override
    public FormatState getFormatState() {
        super.getFormatState();

        formatState.setState(offsetScn.toString());
        return formatState;
    }

    @Override
    public boolean reachedEnd() throws IOException {
        return false;
    }

    @Override
    public InputSplit[] createInputSplits(int minNumSplits) throws IOException {
        return new InputSplit[]{new GenericInputSplit(1,1)};
    }

    @Override
    protected void closeInternal() throws IOException {
        try {
            if(logMinerData != null){
                logMinerData.close();
            }
        } catch (SQLException e) {
            LOG.warn("关闭资源logMinerData出错:", e);
        }

        try {
            if(logMinerSelectStmt != null){
                logMinerSelectStmt.cancel();
                logMinerSelectStmt.close();
            }
        } catch (SQLException e) {
            LOG.warn("关闭资源logMinerSelectStmt出错:", e);
        }

        try {
            if (logMinerStartStmt != null) {
                logMinerStartStmt.close();
            }
        } catch (SQLException e) {
            LOG.warn("关闭资源logMinerStartStmt出错:", e);
        }

        try {
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException e) {
            LOG.warn("关闭资源connection出错:", e);
        }
    }

    enum ReadPosition{
        ALL, CURRENT, TIME, SCN
    }
}
