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

import com.dtstack.chunjun.connector.oraclelogminer.conf.LogMinerConf;
import com.dtstack.chunjun.connector.oraclelogminer.converter.LogMinerColumnConverter;
import com.dtstack.chunjun.connector.oraclelogminer.entity.QueueData;
import com.dtstack.chunjun.connector.oraclelogminer.util.OraUtil;
import com.dtstack.chunjun.converter.AbstractCDCRowConverter;
import com.dtstack.chunjun.element.ErrorMsgRowData;
import com.dtstack.chunjun.util.ExceptionUtil;
import com.dtstack.chunjun.util.RetryUtil;

import org.apache.flink.table.data.RowData;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.sql.DriverManager;
import java.util.LinkedList;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.dtstack.chunjun.connector.oraclelogminer.listener.LogMinerConnection.RETRY_TIMES;
import static com.dtstack.chunjun.connector.oraclelogminer.listener.LogMinerConnection.SLEEP_TIME;

/**
 * @author jiangbo
 * @date 2020/3/27
 */
public class LogMinerListener implements Runnable {

    public static Logger LOG = LoggerFactory.getLogger(LogMinerListener.class);
    private final LogMinerConf logMinerConf;
    private final PositionManager positionManager;
    private final AbstractCDCRowConverter rowConverter;
    private final LogMinerHelper logMinerHelper;
    private BlockingQueue<QueueData> queue;
    private ExecutorService executor;
    private LogParser logParser;
    private boolean running = false;
    private final transient LogMinerListener listener;
    /** 连续接收到错误数据的次数 */
    private int failedTimes = 0;

    public LogMinerListener(
            LogMinerConf logMinerConf,
            PositionManager positionManager,
            AbstractCDCRowConverter rowConverter) {
        this.positionManager = positionManager;
        this.logMinerConf = logMinerConf;
        this.listener = this;
        this.rowConverter = rowConverter;
        this.logMinerHelper = new LogMinerHelper(listener, logMinerConf, null);
    }

    public void init() {
        queue = new LinkedBlockingDeque<>();

        ThreadFactory namedThreadFactory =
                new ThreadFactoryBuilder().setNameFormat("LogMiner-pool-%d").build();
        executor =
                new ThreadPoolExecutor(
                        1,
                        1,
                        0L,
                        TimeUnit.MILLISECONDS,
                        new LinkedBlockingQueue<>(1024),
                        namedThreadFactory,
                        new ThreadPoolExecutor.AbortPolicy());

        logParser = new LogParser(logMinerConf);
    }

    public void start() {
        BigInteger startScn = logMinerHelper.getStartScn(positionManager.getPosition());

        positionManager.updatePosition(startScn);
        logMinerHelper.setStartScn(startScn);
        logMinerHelper.init();

        // LogMinerColumnConverter 需要connection获取元数据
        if (rowConverter instanceof LogMinerColumnConverter) {
            ((LogMinerColumnConverter) rowConverter)
                    .setConnection(
                            RetryUtil.executeWithRetry(
                                    () ->
                                            DriverManager.getConnection(
                                                    logMinerConf.getJdbcUrl(),
                                                    logMinerConf.getUsername(),
                                                    logMinerConf.getPassword()),
                                    RETRY_TIMES,
                                    SLEEP_TIME,
                                    false));
        }

        executor.execute(this);
        running = true;
    }

    @Override
    public void run() {
        Thread.currentThread()
                .setUncaughtExceptionHandler(
                        (t, e) -> {
                            LOG.warn(
                                    "LogMinerListener run failed, Throwable = {}",
                                    ExceptionUtil.getErrorMessage(e));
                            executor.execute(listener);
                            LOG.info("Re-execute LogMinerListener successfully");
                        });

        while (running) {
            QueueData log = null;
            try {
                if (logMinerHelper.hasNext()) {
                    log = logMinerHelper.getQueueData();
                    processData(log);
                }
            } catch (Exception e) {
                sendException(e, log);
                logMinerHelper.restart(e);
            }
        }
    }

    public void sendException(Exception e, QueueData log) {
        StringBuilder sb = new StringBuilder(512);
        sb.append("LogMinerListener thread exception: current scn =")
                .append(positionManager.getPosition());
        if (Objects.nonNull(log)) {
            sb.append(",\nlog = ").append(log);
        }
        sb.append(",\ne = ").append(ExceptionUtil.getErrorMessage(e));
        String msg = sb.toString();
        LOG.warn(msg);
        try {
            queue.put(new QueueData(BigInteger.ZERO, new ErrorMsgRowData(msg)));
            Thread.sleep(2000L);
        } catch (InterruptedException ex) {
            LOG.warn(
                    "error to put exception message into queue, e = {}",
                    ExceptionUtil.getErrorMessage(ex));
        }
    }

    public void stop() {
        if (null != executor && !executor.isShutdown()) {
            executor.shutdown();
            running = false;
        }

        if (null != queue) {
            queue.clear();
        }

        if (null != logMinerHelper) {
            logMinerHelper.stop();
        }
    }

    private void processData(QueueData log) throws Exception {
        LinkedList<RowData> rowDatalist = logParser.parse(log, rowConverter);
        RowData rowData;
        try {
            while ((rowData = rowDatalist.poll()) != null) {
                queue.put(new QueueData(log.getScn(), rowData));
            }
        } catch (Exception e) {
            LOG.error("{}", ExceptionUtil.getErrorMessage(e));
        }
    }

    public RowData getData() {
        RowData rowData = null;
        try {
            // 最多阻塞100ms
            QueueData poll = queue.poll(100, TimeUnit.MILLISECONDS);
            if (Objects.nonNull(poll)) {
                rowData = poll.getData();
                if (rowData instanceof ErrorMsgRowData) {
                    if (++failedTimes >= logMinerConf.getRetryTimes()) {
                        String errorMsg = rowData.toString();
                        StringBuilder sb = new StringBuilder(errorMsg.length() + 128);
                        sb.append("Error data is received ")
                                .append(failedTimes)
                                .append(" times continuously, ");
                        Pair<String, String> pair = OraUtil.parseErrorMsg(errorMsg);
                        if (pair != null) {
                            sb.append("\nthe Cause maybe : ")
                                    .append(pair.getLeft())
                                    .append(", \nand the Solution maybe : ")
                                    .append(pair.getRight())
                                    .append(", ");
                        }
                        sb.append("\nerror msg is : ").append(errorMsg);
                        throw new RuntimeException(sb.toString());
                    }
                    rowData = null;
                } else {
                    positionManager.updatePosition(poll.getScn());
                    failedTimes = 0;
                }
            }
        } catch (InterruptedException e) {
            LOG.warn("Get data from queue error:", e);
        }
        return rowData;
    }

    public BigInteger getCurrentPosition() {
        return positionManager.getPosition();
    }
}
