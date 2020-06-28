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
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.*;

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
        logMinerConnection.checkPrivileges();

        Long startScn = logMinerConnection.getStartScn(positionManager.getPosition());
        positionManager.updatePosition(startScn);

        executor.submit(this);
        running = true;
    }

    @Override
    public void run() {
        while (running) {
            try {
                if (logMinerConnection.hasNext()) {
                    QueueData log = logMinerConnection.next();
                    queue.put(logParser.parse(log));
                } else {
                    logMinerConnection.closeStmt();
                    logMinerConnection.startOrUpdateLogMiner(positionManager.getPosition());
                    logMinerConnection.queryData(positionManager.getPosition());

                    LOG.info("Update log and continue read:{}", positionManager.getPosition());
                }
            } catch (Exception e) {
                running = false;
                Map<String, Object> map = Collections.singletonMap("exception", e);
                try {
                    queue.put(new QueueData(0L, map));
                } catch (InterruptedException ex) {
                    LOG.error("error to put exception message into queue, exception message = {}, e = {}", ExceptionUtil.getErrorMessage(e), ExceptionUtil.getErrorMessage(ex));
                    throw new RuntimeException(ex);
                }
                logMinerConnection.closeStmt();
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
            if (data.getScn() == 0L) {
                throw new RuntimeException((Exception)data.getData().get("exception"));
            }

            positionManager.updatePosition(data.getScn());
            return data.getData();
        } catch (InterruptedException e) {
            LOG.warn("Get data from queue error:", e);
        }

        return null;
    }
}
