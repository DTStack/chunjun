/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.chunjun.connector.starrocks.streamload;

import com.dtstack.chunjun.connector.starrocks.config.StarRocksConfig;

import org.apache.flink.util.Preconditions;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

@Slf4j
public class StreamLoadManager {
    final LinkedBlockingDeque<StarRocksSinkBufferEntity> flushQueue = new LinkedBlockingDeque<>(1);
    private final Map<String, StarRocksSinkBufferEntity> bufferMap = new ConcurrentHashMap<>();
    private final Map<String, List<Map<String, Object>>> structCacheMap = new ConcurrentHashMap<>();

    private final StarRocksConfig starRocksConfig;
    private final boolean __opAutoProjectionInJson;

    private final StarRocksQueryVisitor starrocksQueryVisitor;
    private final StarRocksStreamLoadVisitor starrocksStreamLoadVisitor;

    private volatile boolean closed = false;
    private volatile Throwable flushException;

    public StreamLoadManager(StarRocksConfig starRocksConfig) {
        this.starRocksConfig = starRocksConfig;
        this.starrocksQueryVisitor = new StarRocksQueryVisitor(starRocksConfig);

        String version = starrocksQueryVisitor.getStarRocksVersion();
        __opAutoProjectionInJson = version.length() > 0 && !version.trim().startsWith("1.");
        this.starrocksStreamLoadVisitor = new StarRocksStreamLoadVisitor(starRocksConfig);
    }

    public void write(
            String tableIdentify,
            List<String> columnList,
            List<Map<String, Object>> data,
            boolean checkTableStructure)
            throws Exception {
        try {
            checkFlushException();
            StarRocksSinkBufferEntity bufferEntity =
                    bufferMap.computeIfAbsent(
                            tableIdentify,
                            f -> {
                                String[] databaseAndTable = tableIdentify.split("\\.");
                                StarRocksSinkBufferEntity starRocksSinkBufferEntity =
                                        new StarRocksSinkBufferEntity(
                                                databaseAndTable[0],
                                                databaseAndTable[1],
                                                columnList);
                                if (checkTableStructure) {
                                    validateTableStructure(starRocksSinkBufferEntity);
                                }
                                return starRocksSinkBufferEntity;
                            });
            bufferEntity.addToBuffer(
                    JSON.toJSONString(data).getBytes(StandardCharsets.UTF_8), data.size());
            if (bufferEntity.getBatchCount() >= starRocksConfig.getLoadConfig().getBatchMaxRows()
                    || bufferEntity.getBatchSize()
                            >= starRocksConfig.getLoadConfig().getBatchMaxSize()) {
                log.info(
                        String.format(
                                "StarRocks buffer Sinking triggered: tableIdentify[%s] rows[%d] label[%s].",
                                tableIdentify,
                                bufferEntity.getBatchCount(),
                                bufferEntity.getLabel()));
                flush(tableIdentify, false);
            }
        } catch (Exception e) {
            throw new Exception("Writing records to StarRocks failed.", e);
        }
    }

    private List<Map<String, Object>> getTableStruct(StarRocksSinkBufferEntity entity) {
        if (starRocksConfig.isCacheTableStruct()) {
            return structCacheMap.computeIfAbsent(
                    getStructCacheKey(entity),
                    k -> getTableStructFromSource(entity.getDatabase(), entity.getTable()));
        } else {
            return getTableStructFromSource(entity.getDatabase(), entity.getTable());
        }
    }

    private String getStructCacheKey(StarRocksSinkBufferEntity entity) {
        return entity.getDatabase() + "_" + entity.getTable();
    }

    private List<Map<String, Object>> getTableStructFromSource(String database, String table) {
        return starrocksQueryVisitor.getTableColumnsMetaData(database, table);
    }

    public void validateTableStructure(StarRocksSinkBufferEntity entity) {
        if (starRocksConfig.getLoadConfig().getHeadProperties().containsKey("columns")) {
            return;
        }
        if (starRocksConfig.isCheckStructFirstTime()
                && starRocksConfig.isCacheTableStruct()
                && structCacheMap.containsKey(getStructCacheKey(entity))) {
            return;
        }
        List<Map<String, Object>> rows =
                Preconditions.checkNotNull(
                        getTableStruct(entity),
                        String.format(
                                "Couldn't get the sink table[%s] column info.",
                                getStructCacheKey(entity)));
        validateTableStructure(rows, entity);
    }

    public void validateTableStructure(
            List<Map<String, Object>> rows, StarRocksSinkBufferEntity entity) {
        // validate primary keys
        List<String> primaryKeyList = new ArrayList<>();
        Set<String> containedColumnNameSet = new HashSet<>();
        for (Map<String, Object> row : rows) {
            String keysType = row.get("COLUMN_KEY").toString();
            String column_name = row.get("COLUMN_NAME").toString();
            if (entity.getColumnList().stream().anyMatch(cn -> cn.equalsIgnoreCase(column_name))) {
                containedColumnNameSet.add(column_name);
            }
            if ("PRI".equals(keysType)) {
                primaryKeyList.add(column_name.toLowerCase());
            }
        }
        if (!primaryKeyList.isEmpty()) {
            entity.setSupportDelete(
                    new HashSet<>(entity.getColumnList()).containsAll(primaryKeyList),
                    __opAutoProjectionInJson);
        }
        if (containedColumnNameSet.size() != entity.getColumnList().size()) {
            throw new IllegalArgumentException(
                    String.format(
                            "The columnList:%s contains columns that do not exist in the corresponding table[%s]",
                            entity.getColumnList(),
                            String.format("%s.%s", entity.getDatabase(), entity.getTable())));
        }
    }

    public void flush(String bufferKey, boolean waitUtilDone) throws Exception {
        if (bufferMap.isEmpty()) {
            flushInternal(null, waitUtilDone);
            return;
        }
        if (null == bufferKey) {
            for (String key : bufferMap.keySet()) {
                flushInternal(key, waitUtilDone);
            }
            return;
        }
        flushInternal(bufferKey, waitUtilDone);
    }

    private synchronized void flushInternal(String bufferKey, boolean waitUtilDone)
            throws Exception {
        checkFlushException();
        if (null == bufferKey || bufferMap.isEmpty() || !bufferMap.containsKey(bufferKey)) {
            if (waitUtilDone) {
                waitAsyncFlushingDone();
            }
            return;
        }
        offer(bufferMap.get(bufferKey));
        bufferMap.remove(bufferKey);
        if (waitUtilDone) {
            // wait the last flush
            waitAsyncFlushingDone();
        }
    }

    private void waitAsyncFlushingDone() throws InterruptedException {
        // wait for previous flushings
        offer(new StarRocksSinkBufferEntity(null, null, null));
        checkFlushException();
    }

    void offer(StarRocksSinkBufferEntity bufferEntity) throws InterruptedException {
        if (!flushQueue.offer(
                bufferEntity,
                starRocksConfig.getLoadConfig().getQueueOfferTimeoutMs(),
                TimeUnit.MILLISECONDS)) {
            throw new RuntimeException("Timeout while offering data to flushQueue");
        }
    }

    private void checkFlushException() {
        if (flushException != null) {
            StackTraceElement[] stack = Thread.currentThread().getStackTrace();
            for (StackTraceElement stackTraceElement : stack) {
                log.info(
                        stackTraceElement.getClassName()
                                + "."
                                + stackTraceElement.getMethodName()
                                + " line:"
                                + stackTraceElement.getLineNumber());
            }
            throw new RuntimeException("Writing records to StarRocks failed.", flushException);
        }
    }

    public void startAsyncFlushing() {
        // start flush thread
        Thread flushThread =
                new Thread(
                        () -> {
                            while (true) {
                                try {
                                    if (!asyncFlush()) {
                                        log.info("StarRocks flush failed.");
                                        break;
                                    }
                                } catch (Exception e) {
                                    flushException = e;
                                }
                            }
                        });

        flushThread.setUncaughtExceptionHandler(
                (t, e) -> {
                    log.error(
                            "StarRocks flush thread uncaught exception occurred: " + e.getMessage(),
                            e);
                    flushException = e;
                });
        flushThread.setName("chunjun-starrocks-flush");
        flushThread.setDaemon(true);
        flushThread.start();
    }

    /** @return false if met eof and flush thread will exit. */
    private boolean asyncFlush() throws Exception {
        StarRocksSinkBufferEntity flushData =
                flushQueue.poll(
                        starRocksConfig.getLoadConfig().getQueuePollTimeoutMs(),
                        TimeUnit.MILLISECONDS);
        if (flushData == null || 0 == flushData.getBatchCount()) {
            return true;
        }
        log.info(
                String.format(
                        "Async stream load: db[%s] table[%s] rows[%d] bytes[%d] label[%s].",
                        flushData.getDatabase(),
                        flushData.getTable(),
                        flushData.getBatchCount(),
                        flushData.getBatchSize(),
                        flushData.getLabel()));
        for (int i = 0; i <= starRocksConfig.getMaxRetries(); i++) {
            try {
                starrocksStreamLoadVisitor.doStreamLoad(flushData);
                log.info(
                        String.format(
                                "Async stream load finished: label[%s].", flushData.getLabel()));
                break;
            } catch (Exception e) {
                log.warn("Failed to flush batch data to StarRocks, retry times = {}", i, e);
                if (i >= starRocksConfig.getMaxRetries()) {
                    throw e;
                }
                if (e instanceof StarRocksStreamLoadFailedException
                        && ((StarRocksStreamLoadFailedException) e).needReCreateLabel()) {
                    String oldLabel = flushData.getLabel();
                    flushData.reGenerateLabel();
                    log.warn(
                            String.format(
                                    "Batch label changed from [%s] to [%s]",
                                    oldLabel, flushData.getLabel()));
                }
                try {
                    Thread.sleep(1000L * Math.min(i + 1, 10));
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new IOException(
                            "Unable to flush, interrupted while doing another attempt", e);
                }
            }
        }
        return true;
    }

    public synchronized void close() {
        if (!closed) {
            closed = true;

            if (starrocksQueryVisitor != null) {
                starrocksQueryVisitor.close();
            }

            if (flushException != null) {
                checkFlushException();
                return;
            }
            try {
                log.info("StarRocks Sink is about to close.");
                flush(null, true);
            } catch (Exception e) {
                throw new RuntimeException("Writing records to StarRocks failed.", e);
            }
        }
        checkFlushException();
    }

    public boolean tableHasPartition() {
        return starrocksQueryVisitor.hasPartitions(
                starRocksConfig.getDatabase(), starRocksConfig.getTable());
    }

    public Throwable getFlushException() {
        return flushException;
    }
}
