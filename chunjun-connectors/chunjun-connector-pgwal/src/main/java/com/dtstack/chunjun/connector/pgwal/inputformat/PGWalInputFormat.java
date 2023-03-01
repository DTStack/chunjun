/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dtstack.chunjun.connector.pgwal.inputformat;

import com.dtstack.chunjun.connector.pgwal.conf.PGWalConf;
import com.dtstack.chunjun.connector.pgwal.listener.PgWalListener;
import com.dtstack.chunjun.connector.pgwal.util.PGUtil;
import com.dtstack.chunjun.connector.pgwal.util.ReplicationSlotInfoWrapper;
import com.dtstack.chunjun.converter.AbstractCDCRowConverter;
import com.dtstack.chunjun.element.ErrorMsgRowData;
import com.dtstack.chunjun.restore.FormatState;
import com.dtstack.chunjun.source.format.BaseRichInputFormat;
import com.dtstack.chunjun.util.ExceptionUtil;
import com.dtstack.chunjun.util.RetryUtil;

import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.table.data.RowData;

import org.apache.commons.lang3.StringUtils;
import org.postgresql.PGConnection;
import org.postgresql.jdbc.PgConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/** */
public class PGWalInputFormat extends BaseRichInputFormat {
    private static final Logger LOG = LoggerFactory.getLogger(PGWalInputFormat.class);
    private static Thread.UncaughtExceptionHandler exceptionHandler =
            (t, e) -> LOG.error(e.getMessage(), e);
    private static AtomicInteger threadNumber = new AtomicInteger(0);
    private PGWalConf conf;
    private List<String> categories = new ArrayList<>();
    private AbstractCDCRowConverter rowConverter;
    private transient BlockingQueue<RowData> queue;
    private transient ExecutorService executor;
    private volatile long startLsn;
    private volatile boolean running = false;

    @Override
    public InputSplit[] createInputSplitsInternal(int minNumSplits) {
        InputSplit[] splits = new InputSplit[minNumSplits];
        for (int i = 0; i < minNumSplits; i++) {
            splits[i] = new GenericInputSplit(i, minNumSplits);
        }
        return splits;
    }

    @Override
    public void openInputFormat() throws IOException {
        super.openInputFormat();
        LOG.debug("");
        executor =
                Executors.newFixedThreadPool(
                        1,
                        r -> {
                            Thread t =
                                    new Thread(
                                            r, "PG WAL IO-" + threadNumber.getAndIncrement() + 0);
                            if (t.getPriority() != Thread.NORM_PRIORITY) {
                                t.setPriority(Thread.NORM_PRIORITY);
                            }

                            t.setUncaughtExceptionHandler(exceptionHandler);
                            return t;
                        });
        queue = new ArrayBlockingQueue<>(2 << 10);
    }

    @Override
    protected void openInternal(InputSplit inputSplit) {
        if (inputSplit.getSplitNumber() != 0) {
            LOG.info("pg wal openInternal split number:{} abort...", inputSplit.getSplitNumber());
            return;
        }

        if (StringUtils.isBlank(conf.getSlotName())) {
            conf.setSlotName(PGUtil.SLOT_PRE + jobId);
        }
        PGConnection conn =
                RetryUtil.executeWithRetry(
                        () ->
                                PGUtil.getConnection(
                                        conf.getJdbcUrl(), conf.getUsername(), conf.getPassword()),
                        PGUtil.RETRY_TIMES,
                        PGUtil.SLEEP_TIME,
                        true);

        ReplicationSlotInfoWrapper availableSlot =
                PGUtil.checkPostgres(
                        (PgConnection) conn,
                        conf.getAllowCreated(),
                        conf.getSlotName(),
                        conf.getTables());
        if (availableSlot == null) {
            try {
                PGUtil.createSlot(conn, conf.getSlotName(), conf.getTemp());
            } catch (SQLException e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        }

        if (conf.getLsn() != 0) {
            startLsn = conf.getLsn();
        } else if (formatState != null && formatState.getState() != null) {
            startLsn = (long) formatState.getState();
        }

        LOG.info("pg wal openInternal split number:{} start...", inputSplit.getSplitNumber());
        LOG.info("pg wal config:{}", conf.toString());

        try {
            executor.submit(new PgWalListener(this));
        } catch (SQLException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
        LOG.info("pg cdc started");
        running = true;
    }

    @Override
    public FormatState getFormatState() {
        super.getFormatState();
        if (formatState != null) {
            formatState.setState(startLsn);
        }
        return formatState;
    }

    @Override
    protected RowData nextRecordInternal(RowData row) {
        try {
            RowData rowData = queue.take();
            //            if (map.size() == 1) {
            //                throw new IOException((String) map.get("e"));
            //            } else {
            //                startLsn = (long) map.get("lsn");
            //                row = Row.of(map);
            //            }
            return rowData;
        } catch (InterruptedException e) {
            LOG.error("takeEvent interrupted error:{}", ExceptionUtil.getErrorMessage(e));
        }
        return null;
    }

    @Override
    protected void closeInternal() {
        if (running) {
            executor.shutdownNow();
            running = false;
            LOG.warn("shutdown SqlServerCdcListener......");
        }
    }

    @Override
    public boolean reachedEnd() {
        return false;
    }

    public PGWalConf getConf() {
        return conf;
    }

    public void setConf(PGWalConf conf) {
        this.conf = conf;
    }

    public AbstractCDCRowConverter getRowConverter() {
        return rowConverter;
    }

    public void setRowConverter(AbstractCDCRowConverter rowConverter) {
        this.rowConverter = rowConverter;
    }

    public boolean isRunning() {
        return running;
    }

    public long getStartLsn() {
        return startLsn;
    }

    public void processException(Exception e) {
        queue.add(new ErrorMsgRowData(e.getMessage()));
    }

    public void appendResult(List<RowData> rowData) {
        queue.addAll(rowData);
    }
}
