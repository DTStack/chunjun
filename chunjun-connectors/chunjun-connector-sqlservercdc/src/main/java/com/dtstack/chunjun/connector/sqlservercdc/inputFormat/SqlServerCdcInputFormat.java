/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dtstack.chunjun.connector.sqlservercdc.inputFormat;

import com.dtstack.chunjun.connector.sqlservercdc.config.SqlServerCdcConfig;
import com.dtstack.chunjun.connector.sqlservercdc.entity.Lsn;
import com.dtstack.chunjun.connector.sqlservercdc.entity.TxLogPosition;
import com.dtstack.chunjun.connector.sqlservercdc.listener.SqlServerCdcListener;
import com.dtstack.chunjun.connector.sqlservercdc.util.SqlServerCdcUtil;
import com.dtstack.chunjun.converter.AbstractCDCRawTypeMapper;
import com.dtstack.chunjun.restore.FormatState;
import com.dtstack.chunjun.source.format.BaseRichInputFormat;
import com.dtstack.chunjun.throwable.ReadRecordException;
import com.dtstack.chunjun.util.ClassUtil;
import com.dtstack.chunjun.util.ExceptionUtil;

import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.table.data.RowData;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.dtstack.chunjun.connector.sqlservercdc.util.SqlServerCdcUtil.DRIVER;

@Slf4j
public class SqlServerCdcInputFormat extends BaseRichInputFormat {

    private static final long serialVersionUID = -5098916364083806918L;

    public SqlServerCdcConfig sqlserverCdcConfig;

    private Connection conn;

    private TxLogPosition logPosition;

    private transient LinkedBlockingDeque<RowData> queue;

    private transient ExecutorService executor;

    private volatile boolean running = false;

    private AbstractCDCRawTypeMapper rowConverter;

    @Override
    protected void openInternal(InputSplit inputSplit) {
        ThreadFactory namedThreadFactory =
                new ThreadFactoryBuilder().setNameFormat("cdcListener-pool-%d").build();
        executor =
                new ThreadPoolExecutor(
                        1,
                        1,
                        0L,
                        TimeUnit.MILLISECONDS,
                        new LinkedBlockingQueue<>(1024),
                        namedThreadFactory,
                        new ThreadPoolExecutor.AbortPolicy());
        queue = new LinkedBlockingDeque<>(1000);

        if (inputSplit.getSplitNumber() != 0) {
            log.info(
                    "sqlServer cdc openInternal split number:{} abort...",
                    inputSplit.getSplitNumber());
            return;
        }

        log.info(
                "sqlServer cdc openInternal split number:{} start...", inputSplit.getSplitNumber());
        try {
            ClassUtil.forName(DRIVER, getClass().getClassLoader());
            conn =
                    SqlServerCdcUtil.getConnection(
                            sqlserverCdcConfig.getUrl(),
                            sqlserverCdcConfig.getUsername(),
                            sqlserverCdcConfig.getPassword());
            conn.setAutoCommit(sqlserverCdcConfig.isAutoCommit());
            SqlServerCdcUtil.changeDatabase(conn, sqlserverCdcConfig.getDatabaseName());

            if (StringUtils.isNotBlank(sqlserverCdcConfig.getLsn())) {
                logPosition = TxLogPosition.valueOf(Lsn.valueOf(sqlserverCdcConfig.getLsn()));
            } else if (formatState != null && formatState.getState() != null) {
                logPosition = (TxLogPosition) formatState.getState();
            } else {
                logPosition = TxLogPosition.valueOf(SqlServerCdcUtil.getMaxLsn(conn));
            }

            executor.submit(new SqlServerCdcListener(this));
            running = true;
        } catch (Exception e) {
            log.error(
                    "SqlserverCdcInputFormat open() failed, e = {}",
                    ExceptionUtil.getErrorMessage(e));
            throw new RuntimeException(
                    "SqlserverCdcInputFormat open() failed, e = "
                            + ExceptionUtil.getErrorMessage(e));
        }

        log.info("SqlserverCdcInputFormat[{}]open: end", jobName);
    }

    @Override
    protected RowData nextRecordInternal(RowData row) throws ReadRecordException {
        try {
            return queue.poll(100, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            log.error("takeEvent interrupted error:{}", ExceptionUtil.getErrorMessage(e));
            throw new ReadRecordException("takeEvent interrupted error", e);
        }
    }

    @Override
    protected void closeInternal() {
        if (running) {
            executor.shutdownNow();
            running = false;
            log.warn("shutdown SqlServerCdcListener......");
        }
    }

    @Override
    public InputSplit[] createInputSplitsInternal(int minNumSplits) {
        InputSplit[] splits = new InputSplit[minNumSplits];
        for (int i = 0; i < minNumSplits; i++) {
            splits[i] = new GenericInputSplit(i, minNumSplits);
        }
        return splits;
    }

    @Override
    public boolean reachedEnd() {
        return false;
    }

    @Override
    public FormatState getFormatState() {
        super.getFormatState();
        if (formatState != null) {
            formatState.setState(logPosition);
        }
        return formatState;
    }

    public Connection getConn() {
        return conn;
    }

    public void setLogPosition(TxLogPosition logPosition) {
        this.logPosition = logPosition;
    }

    public TxLogPosition getLogPosition() {
        return logPosition;
    }

    public AbstractCDCRawTypeMapper getCdcRowConverter() {
        return rowConverter;
    }

    public void setRowConverter(AbstractCDCRawTypeMapper rowConverter) {
        this.rowConverter = rowConverter;
    }

    public BlockingQueue<RowData> getQueue() {
        return queue;
    }

    public void setSqlServerCdcConf(SqlServerCdcConfig sqlserverCdcConfig) {
        this.sqlserverCdcConfig = sqlserverCdcConfig;
    }
}
