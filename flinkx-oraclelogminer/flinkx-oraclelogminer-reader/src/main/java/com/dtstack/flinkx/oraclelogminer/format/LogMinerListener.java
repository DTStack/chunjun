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
import com.dtstack.flinkx.util.ExceptionUtil;
import com.dtstack.flinkx.util.GsonUtil;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import net.sf.jsqlparser.JSQLParserException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
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

    /**
     * 连续接收到错误数据的次数
     */
    private int failedTimes;

    public LogMinerListener(LogMinerConfig logMinerConfig, PositionManager positionManager) {
        this.positionManager = positionManager;
        this.logMinerConfig = logMinerConfig;
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
        logMinerConnection.queryOracleVersion();

        Long startScn = logMinerConnection.getStartScn(positionManager.getPosition());
        positionManager.updatePosition(startScn);

        executor.submit(this);
        running = true;
    }

    @Override
    public void run() {
        while (running) {
            QueueData log = null;
            try {
                if (logMinerConnection.hasNext()) {
                    log = logMinerConnection.next();
                    queue.put(logParser.parse(log));
                } else {
                    logMinerConnection.closeStmt();
                    logMinerConnection.startOrUpdateLogMiner(positionManager.getPosition());
                    logMinerConnection.queryData(positionManager.getPosition());
                    LOG.debug("Update log and continue read:{}", positionManager.getPosition());
                }
            } catch (Exception e) {
                if (e instanceof JSQLParserException) {
                    LOG.warn("log parse fail,log is --->{}", log);
                }
                Map<String, Object> map = Collections.singletonMap("e", ExceptionUtil.getErrorMessage(e));
                try {
                    queue.put(new QueueData(0L, map));
                    Thread.sleep(2000L);
                } catch (InterruptedException ex) {
                    LOG.error("error to put exception message into queue, exception message = {}, e = {}", ExceptionUtil.getErrorMessage(e), ExceptionUtil.getErrorMessage(ex));
                }
                //如果连接不正常 需要关闭连接 并重新进行连接
                if (!logMinerConnection.isValid()) {
                    try {
                        logMinerConnection.disConnect();
                    } catch (SQLException throwables) {
                        LOG.error("logminerThread disConnect exception,exception message  = {}, e = {}", ExceptionUtil.getErrorMessage(e), ExceptionUtil.getErrorMessage(throwables));
                    }
                    try {
                        logMinerConnection.connect();
                    } catch (Exception throeables) {
                        LOG.error("logminerThread get connect exception,exception message  = {}, e = {}", ExceptionUtil.getErrorMessage(e), ExceptionUtil.getErrorMessage(throeables));
                    }
                }
            }
        }
    }

    public void stop() throws Exception {
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
            if (data.getScn() != 0L) {
                positionManager.updatePosition(data.getScn());
                failedTimes = 0;
                return data.getData();
            }
            String message = String.format( "LogMinerListener obtain an error data, data = %s", GsonUtil.GSON.toJson(data));
            LOG.error(message);
            if(++failedTimes > 3){
                throw new RuntimeException("Error data is received 3 times continuously,error info->"+message);
            }
        } catch (InterruptedException e) {
            LOG.warn("Get data from queue error:", e);
        }

        return null;
    }
}
