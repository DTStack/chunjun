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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;

/**
 * @author jiangbo
 * @date 2020/3/27
 */
public class LogMinerListener {

    public static Logger LOG = LoggerFactory.getLogger(LogMinerListener.class);

    private BlockingQueue<Map<String, Object>> queue;

    private ExecutorService executor;

    private LogMinerConfig logMinerConfig;

    private PositionManager positionManager;

    private OracleLogMinerInputFormat inputFormat;

    public LogMinerListener(OracleLogMinerInputFormat inputFormat) {
        this.inputFormat = inputFormat;
        logMinerConfig = inputFormat.logMinerConfig;
    }

    public void start() {

    }

    private void run() {
        while (true) {

        }
    }

    public void stop() {
        if (null != executor && executor.isShutdown()) {
            executor.shutdown();
        }

        if (null != queue) {
            queue.clear();
        }
    }

    public Map<String, Object> getData() {
        try {
            return queue.take();
        } catch (InterruptedException e) {
            LOG.warn("Get data from queue error:", e);
        }

        return null;
    }
}
