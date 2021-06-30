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


package com.dtstack.flinkx.connector.oraclelogminer.listener;

import org.apache.flink.table.data.RowData;

import com.dtstack.flinkx.connector.oraclelogminer.conf.LogMinerConf;
import com.dtstack.flinkx.connector.oraclelogminer.converter.LogMinerColumnConverter;
import com.dtstack.flinkx.connector.oraclelogminer.entity.QueueData;
import com.dtstack.flinkx.connector.oraclelogminer.util.OraUtil;
import com.dtstack.flinkx.connector.oraclelogminer.util.SqlUtil;
import com.dtstack.flinkx.converter.AbstractCDCRowConverter;
import com.dtstack.flinkx.element.ErrorMsgRowData;
import com.dtstack.flinkx.util.ExceptionUtil;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import net.sf.jsqlparser.JSQLParserException;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author jiangbo
 * @date 2020/3/27
 */
public class LogMinerListener implements Runnable {

    public static Logger LOG = LoggerFactory.getLogger(LogMinerListener.class);

    private BlockingQueue<QueueData> queue;

    private ExecutorService executor;

    private final LogMinerConf logMinerConf;

    private LogMinerConnection logMinerConnection;

    private final PositionManager positionManager;

    private LogParser logParser;

    private boolean running = false;

    private String logMinerSelectSql;

    private transient LogMinerListener listener;

    private final AbstractCDCRowConverter rowConverter;
    /**
     * 连续接收到错误数据的次数
     */
    private int failedTimes = 0;

    public LogMinerListener(LogMinerConf logMinerConf, PositionManager positionManager, AbstractCDCRowConverter rowConverter) {
        this.positionManager = positionManager;
        this.logMinerConf = logMinerConf;
        this.listener = this;
        this.rowConverter = rowConverter;
    }

    public void init() {
        queue = new LinkedBlockingDeque<>();

        ThreadFactory namedThreadFactory = new ThreadFactoryBuilder().setNameFormat("LogMiner-pool-%d").build();
        executor = new ThreadPoolExecutor(1,
                1,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(1024),
                namedThreadFactory,
                new ThreadPoolExecutor.AbortPolicy());

        logMinerConnection = new LogMinerConnection(logMinerConf);
        logParser = new LogParser(logMinerConf);
    }

    public void start() {
        logMinerConnection.connect();
        logMinerConnection.checkPrivileges();

        Long startScn = logMinerConnection.getStartScn(positionManager.getPosition());
        logMinerConnection.setPreScn(startScn);
        positionManager.updatePosition(startScn);

        logMinerSelectSql = SqlUtil.buildSelectSql(logMinerConf.getCat(), logMinerConf.getListenerTables());

        //LogMinerColumnConverter 需要connection获取元数据
        if (rowConverter instanceof LogMinerColumnConverter) {
            ((LogMinerColumnConverter) rowConverter).setConnection(logMinerConnection);
        }


        executor.execute(this);
        running = true;
    }

    @Override
    public void run() {
        Thread.currentThread().setUncaughtExceptionHandler((t, e) -> {
            LOG.warn("LogMinerListener run failed, Throwable = {}", ExceptionUtil.getErrorMessage(e));
            executor.execute(listener);
            LOG.info("Re-execute LogMinerListener successfully");
        });

        while (running) {
            QueueData log = null;
            try {
                if (logMinerConnection.hasNext()) {
                    log = logMinerConnection.next();
                    processData(log);
                } else {
                    logMinerConnection.closeStmt();
                    logMinerConnection.startOrUpdateLogMiner(positionManager.getPosition());
                    logMinerConnection.queryData(positionManager.getPosition(), logMinerSelectSql);
                    LOG.debug("Update log and continue read:{}", positionManager.getPosition());
                }
            } catch (Exception e) {
                StringBuilder sb = new StringBuilder(512);
                sb.append("LogMinerListener thread exception: current scn =")
                        .append(positionManager.getPosition());
                if (e instanceof JSQLParserException) {
                    sb.append(",\nlog = ").append(log);
                }
                sb.append(",\ne = ").append(ExceptionUtil.getErrorMessage(e));
                String msg = sb.toString();
                LOG.warn(msg);

                //如果连续重试三次 就直接任务失败
                if(++failedTimes >= 3){
                    StringBuilder errorMsg = new StringBuilder(msg + 128);
                    errorMsg.append("Error data is received 3 times continuously, ");
                    Pair<String, String> pair = OraUtil.parseErrorMsg(msg);
                    if (pair != null) {
                        errorMsg.append("\nthe Cause maybe : ")
                                .append(pair.getLeft())
                                .append(", \nand the Solution maybe : ")
                                .append(pair.getRight())
                                .append(", ");
                    }
                    errorMsg.append("\nerror msg is : ").append(sb);
                    try {
                        queue.put(new QueueData(0L,new ErrorMsgRowData(errorMsg.toString())));
                    } catch (InterruptedException ex) {
                        LOG.warn("error to put exception message into queue, e = {}", ExceptionUtil.getErrorMessage(ex));
                    }
                }else{
                    try {
                        logMinerConnection.disConnect();
                    } catch (Exception e1) {
                        LOG.warn("LogMiner Thread disConnect exception, e = {}", ExceptionUtil.getErrorMessage(e1));
                    }

                    logMinerConnection.connect();
                }
            }
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

        if (null != logMinerConnection) {
            logMinerConnection.disConnect();
        }
    }

    private void processData(QueueData log) throws Exception {
        LinkedList<RowData> rowDatalist = logParser.parse(log, logMinerConnection.isOracle10, rowConverter);
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
            //最多阻塞100ms
            QueueData poll = queue.poll(100, TimeUnit.MILLISECONDS);
            if(Objects.nonNull(poll)){
                rowData = poll.getData();
                if (rowData instanceof ErrorMsgRowData) {
                    throw new RuntimeException(rowData.toString());
                }
                positionManager.updatePosition( poll.getScn());
            }
        } catch (InterruptedException e) {
            LOG.warn("Get data from queue error:", e);
        }
        return rowData;
    }
}
