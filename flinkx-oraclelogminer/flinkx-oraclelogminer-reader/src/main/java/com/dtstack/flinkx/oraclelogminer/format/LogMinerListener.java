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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
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

    private BlockingQueue<Pair<Long, Map<String, Object>>> queue;

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
                    Pair<Long, Map<String, Object>> log = logMinerConnection.next();
                    queue.add(logParser.parse(log));
                } else {
                    logMinerConnection.startOrUpdateLogMiner(positionManager.getPosition());
                    logMinerConnection.queryData(positionManager.getPosition());

                    System.out.println("Update log and continue read:" + positionManager.getPosition());
                    LOG.info("Update log and continue read:{}", positionManager.getPosition());
                }
            } catch (Exception e) {
                running = false;

                Map<String, Object> map = new HashMap<>(1);
                map.put("exception", e);
                queue.add(Pair.of(0L, map));
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
            Pair<Long, Map<String, Object>> pair = queue.take();
            if (pair.getLeft() == 0L) {
                throw new RuntimeException((Exception)pair.getRight().get("exception"));
            }

            positionManager.updatePosition(pair.getLeft());
            return pair.getRight();
        } catch (InterruptedException e) {
            LOG.warn("Get data from queue error:", e);
        }

        return null;
    }
}
