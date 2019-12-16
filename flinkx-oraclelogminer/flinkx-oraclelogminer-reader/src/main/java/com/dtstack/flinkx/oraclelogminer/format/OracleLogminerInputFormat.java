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
import com.dtstack.flinkx.oraclelogminer.util.LogminerUtil;
import com.dtstack.flinkx.util.ClassUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.sql.*;

/**
 * @author jiangbo
 * @date 2019/12/14
 */
public class OracleLogminerInputFormat extends RichInputFormat {

    protected LogminerConfig logminerConfig;

    private Connection connection;

    private CallableStatement logMinerStartStmt;

    private PreparedStatement logMinerSelectStmt;

    private ResultSet logMinerData;

    private long offsetSCN;

    @Override
    public void configure(Configuration parameters) {
        // do nothing
    }

    @Override
    public void openInputFormat() throws IOException {
        super.openInputFormat();

        try {
            ClassUtil.forName(logminerConfig.getDriverName(), getClass().getClassLoader());
            connection = DriverManager.getConnection(logminerConfig.getJdbcUrl(), logminerConfig.getUsername(), logminerConfig.getPassword());

            LOG.info("获取连接成功,url:{}, username:{}", logminerConfig.getJdbcUrl(), logminerConfig.getUsername());
        } catch (SQLException e){
            LOG.error("获取连接失败，url:{}, username:{}", logminerConfig.getJdbcUrl(), logminerConfig.getUsername());
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void openInternal(InputSplit inputSplit) throws IOException {
        initOffsetSCN();
        startLogminer();
        startSelectData();
    }

    private void initOffsetSCN(){
        // TODO
    }

    private void startLogminer(){
        try {
            logMinerStartStmt = connection.prepareCall(LogminerUtil.SQL_START_LOGMINER);
            logMinerStartStmt.setLong(1, offsetSCN);
            logMinerStartStmt.execute();

            LOG.info("启动Log miner成功,offset:{}， sql:{}", offsetSCN, LogminerUtil.SQL_START_LOGMINER);
        } catch (SQLException e){
            LOG.error("启动Log miner失败,offset:{}， sql:{}", offsetSCN, LogminerUtil.SQL_START_LOGMINER);
            throw new RuntimeException(e);
        }
    }

    private void startSelectData() {
        String logMinerSelectSql = LogminerUtil.buildSelectSql(logminerConfig.getListenerOperations(), logminerConfig.getListenerTables());
        try {
            logMinerSelectStmt = connection.prepareStatement(logMinerSelectSql);
            logMinerSelectStmt.setFetchSize(logminerConfig.getFetchSize());
            logMinerSelectStmt.setLong(1, 1L);
            logMinerData = logMinerSelectStmt.executeQuery();

            LOG.info("查询Log miner数据,sql:{}", logMinerSelectSql);
        } catch (SQLException e) {
            LOG.error("查询Log miner数据出错,sql:{}", "");
            throw new RuntimeException(e);
        }
    }

    @Override
    protected Row nextRecordInternal(Row row) throws IOException {
        return null;
    }

    @Override
    public boolean reachedEnd() throws IOException {
        return false;
    }

    @Override
    public InputSplit[] createInputSplits(int minNumSplits) throws IOException {

        return new InputSplit[0];
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
