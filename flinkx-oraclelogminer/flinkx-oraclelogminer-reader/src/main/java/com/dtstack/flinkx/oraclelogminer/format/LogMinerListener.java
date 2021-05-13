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
import com.dtstack.flinkx.oraclelogminer.util.OraUtil;
import com.dtstack.flinkx.oraclelogminer.util.SqlUtil;
import com.dtstack.flinkx.util.ExceptionUtil;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import net.sf.jsqlparser.JSQLParserException;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
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

    private LogMinerConfig logMinerConfig;

    private LogMinerConnection logMinerConnection;

    private PositionManager positionManager;

    private LogParser logParser;

    private boolean running = false;

    private String logMinerSelectSql;

    private transient LogMinerListener listener;

    /**
     * 连续接收到错误数据的次数
     */
    private int failedTimes;

    public LogMinerListener(LogMinerConfig logMinerConfig, PositionManager positionManager) {
        this.positionManager = positionManager;
        this.logMinerConfig = logMinerConfig;
        this.listener = this;
    }

    public void init() {
        queue = new SynchronousQueue<>(false);

        ThreadFactory namedThreadFactory = new ThreadFactoryBuilder().setNameFormat("LogMiner-pool-%d").build();
        executor = new ThreadPoolExecutor(1,
                1,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(1024),
                namedThreadFactory,
                new ThreadPoolExecutor.AbortPolicy());

        logMinerConnection = new LogMinerConnection(logMinerConfig);
        logParser = new LogParser(logMinerConfig);
    }

    public void start() {
        logMinerConnection.connect();
        logMinerConnection.checkPrivileges();

        BigDecimal startScn = logMinerConnection.getStartScn(positionManager.getPosition());
        positionManager.updatePosition(startScn);

        logMinerSelectSql = SqlUtil.buildSelectSql(logMinerConfig.getCat(), logMinerConfig.getListenerTables());
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
                    queue.put(logParser.parse(log));
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
                try {
                    queue.put(new QueueData(BigDecimal.ZERO, Collections.singletonMap("e", msg)));
                    Thread.sleep(2000L);
                } catch (InterruptedException ex) {
                    LOG.warn("error to put exception message into queue, e = {}", ExceptionUtil.getErrorMessage(ex));
                }
                try {
                    logMinerConnection.disConnect();
                } catch (Exception e1) {
                    LOG.warn("LogMiner Thread disConnect exception, e = {}", ExceptionUtil.getErrorMessage(e1));
                }

                logMinerConnection.connect();
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

    public Map<String, Object> getData() {
        try {
            QueueData data = queue.take();
            if (data.getScn().compareTo(BigDecimal.ZERO)!= 0) {
                positionManager.updatePosition(data.getScn());
                failedTimes = 0;
                return data.getData();
            }
            if (++failedTimes >= 3) {
                String errorMsg = (String)data.getData().get("e");
                StringBuilder sb = new StringBuilder(errorMsg.length() + 128);
                sb.append("Error data is received 3 times continuously, ");
                Pair<String, String> pair = OraUtil.parseErrorMsg(errorMsg);
                if(pair != null){
                    sb.append("\nthe Cause maybe : ")
                            .append(pair.getLeft())
                            .append(", \nand the Solution maybe : ")
                            .append(pair.getRight())
                            .append(", ");
                }
                sb.append("\nerror msg is : ").append(errorMsg);
                throw new RuntimeException(sb.toString());
            }
        } catch (InterruptedException e) {
            LOG.warn("Get data from queue error:", e);
        }

        return null;
    }
}
