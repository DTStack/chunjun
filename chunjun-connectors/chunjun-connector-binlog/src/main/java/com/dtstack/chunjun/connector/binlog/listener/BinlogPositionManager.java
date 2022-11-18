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

import com.dtstack.chunjun.connector.binlog.inputformat.BinlogInputFormat;

import com.alibaba.otter.canal.parse.exception.CanalParseException;
import com.alibaba.otter.canal.parse.index.AbstractLogPositionManager;
import com.alibaba.otter.canal.protocol.position.LogPosition;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

public class BinlogPositionManager extends AbstractLogPositionManager {

    private final BinlogInputFormat format;
    private final Cache<String, LogPosition> logPositionCache;

    public BinlogPositionManager(BinlogInputFormat format) {
        this.format = format;
        logPositionCache = CacheBuilder.newBuilder().build();
    }

    @Override
    public LogPosition getLatestIndexBy(String destination) {
        return logPositionCache.getIfPresent(destination);
    }

    @Override
    public void persistLogPosition(String destination, LogPosition logPosition)
            throws CanalParseException {
        format.setEntryPosition(logPosition.getPostion());
        logPositionCache.put(destination, logPosition);
    }

    @Override
    public void stop() {
        super.stop();
        logPositionCache.cleanUp();
    }
}
