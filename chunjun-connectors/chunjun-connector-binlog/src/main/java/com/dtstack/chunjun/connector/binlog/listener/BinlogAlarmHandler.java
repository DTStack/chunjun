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
package com.dtstack.chunjun.connector.binlog.listener;

import com.alibaba.otter.canal.common.AbstractCanalLifeCycle;
import com.alibaba.otter.canal.common.alarm.CanalAlarmHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BinlogAlarmHandler extends AbstractCanalLifeCycle implements CanalAlarmHandler {

    private static final Logger logger = LoggerFactory.getLogger(BinlogAlarmHandler.class);
    private static final String BINARY_LOG_LOST =
            "Could not find first log file name in binary log index file";

    @Override
    public void sendAlarm(String destination, String msg) {
        logger.error("destination:{}[{}]", destination, msg);
        if (msg.contains(BINARY_LOG_LOST)) {
            System.exit(-1);
        }
    }
}
