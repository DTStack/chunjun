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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.sql.*;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author jiangbo
 * @date 2019/12/14
 */
public class OracleLogMinerInputFormat extends RichInputFormat {

    public LogMinerConfig logMinerConfig;

    private transient Connection connection;

    private transient CallableStatement logMinerStartStmt;

    private transient PreparedStatement logMinerSelectStmt;

    private transient ResultSet logMinerData;

    private long offsetSCN;

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
        initOffsetSCN();
        startLogMiner();
        startSelectData();
    }

    private void initOffsetSCN(){
        // TODO
        getLastScn();
    }

    private void getLastScn(){
        try {
            CallableStatement currentSCNStmt = connection.prepareCall("select min(current_scn) CURRENT_SCN from gv$database");
            ResultSet currentScnResultSet = currentSCNStmt.executeQuery();
            while(currentScnResultSet.next()){
                offsetSCN = currentScnResultSet.getLong("CURRENT_SCN");
            }

            currentScnResultSet.close();
            currentSCNStmt.close();
        } catch (Exception e) {
            LOG.warn("", e);
        }
    }

    private void startLogMiner(){
        try {
            logMinerStartStmt = connection.prepareCall(LogMinerUtil.SQL_START_LOGMINER);
            logMinerStartStmt.setLong(1, offsetSCN);
            logMinerStartStmt.execute();

            LOG.info("启动Log miner成功,offset:{}， sql:{}", offsetSCN, LogMinerUtil.SQL_START_LOGMINER);
        } catch (SQLException e){
            LOG.error("启动Log miner失败,offset:{}， sql:{}", offsetSCN, LogMinerUtil.SQL_START_LOGMINER);
            throw new RuntimeException(e);
        }
    }

    private void startSelectData() {
        String logMinerSelectSql = LogMinerUtil.buildSelectSql(logMinerConfig.getListenerOperations(), logMinerConfig.getListenerTables());
        try {
            logMinerSelectStmt = connection.prepareStatement(logMinerSelectSql);
            logMinerSelectStmt.setFetchSize(logMinerConfig.getFetchSize());
            logMinerSelectStmt.setLong(1, 1L);
            logMinerData = logMinerSelectStmt.executeQuery();

            LOG.info("查询Log miner数据,sql:{}", logMinerSelectSql);
        } catch (SQLException e) {
            LOG.error("查询Log miner数据出错,sql:{}", logMinerSelectSql);
            throw new RuntimeException(e);
        }
    }

    @Override
    protected Row nextRecordInternal(Row row) throws IOException {
        String sqlRedo = "";
        try {
            String schema = logMinerData.getString("SEG_OWNER");
            String tableName = logMinerData.getString("TABLE_NAME");
            String operation = logMinerData.getString("OPERATION");
            sqlRedo = logMinerData.getString("SQL_REDO");
            if (sqlRedo.contains("temporary tables")){
                return null;
            }

            boolean contSF = logMinerData.getBoolean("CSF");
            while(contSF){
                logMinerData.next();
                sqlRedo += logMinerData.getString("SQL_REDO");
                contSF = logMinerData.getBoolean("CSF");
            }

            return LogMinerUtil.parseSql(schema, tableName, sqlRedo);
        } catch (Exception e) {
            LOG.error("", e);
        }

        return Row.of(sqlRedo);
    }

    @Override
    public FormatState getFormatState() {
        return super.getFormatState();

    }

    @Override
    public boolean reachedEnd() throws IOException {
        try {
            return !logMinerData.next();
        } catch (SQLException e) {
            LOG.error("获取下一条数据出错:", e);
            throw new RuntimeException(e);
        }
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
}
