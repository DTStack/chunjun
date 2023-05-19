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
package com.dtstack.chunjun.connector.jdbc.sink.wrapper.proxy;

import com.dtstack.chunjun.connector.jdbc.sink.wrapper.JdbcBatchStatementWrapper;

import org.apache.flink.table.data.RowData;

import com.esotericsoftware.minlog.Log;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public abstract class CachedWrapperProxy implements JdbcBatchStatementWrapper<RowData> {

    protected Connection connection;
    private final int cacheSize;
    private final long cacheDurationMin;

    protected Cache<String, JdbcBatchStatementWrapper<RowData>> cache;

    protected JdbcBatchStatementWrapper<RowData> currentExecutor;

    /** 当调用writeMultipleRecords 可能会涉及到多个executor */
    protected final Set<JdbcBatchStatementWrapper<RowData>> unExecutedExecutor =
            new LinkedHashSet<>();

    public CachedWrapperProxy(
            Connection connection, int cacheSize, long cacheDurationMin, boolean cacheIsExpire) {
        this.connection = connection;
        this.cacheSize = cacheSize;
        this.cacheDurationMin = cacheDurationMin;
        initCache(cacheIsExpire);
    }

    @Override
    public final void addToBatch(RowData record) throws Exception {
        record = switchExecutorFromCache(record);
        currentExecutor.addToBatch(record);
        unExecutedExecutor.add(currentExecutor);
    }

    @Override
    public final void writeSingleRecord(RowData record) throws Exception {
        record = switchExecutorFromCache(record);
        currentExecutor.writeSingleRecord(record);
    }

    @Override
    public final ResultSet executeQuery(RowData record) throws Exception {
        record = switchExecutorFromCache(record);
        return currentExecutor.executeQuery(record);
    }

    @Override
    public final void clearParameters() throws SQLException {
        currentExecutor.clearParameters();
    }

    @Override
    public final void executeBatch() throws Exception {
        for (JdbcBatchStatementWrapper<RowData> executor : unExecutedExecutor) {
            executor.executeBatch();
        }
    }

    @Override
    public final void clearBatch() throws SQLException {
        for (JdbcBatchStatementWrapper<RowData> executor : unExecutedExecutor) {
            executor.clearBatch();
        }
        unExecutedExecutor.clear();
    }

    @Override
    public final void close() throws SQLException {
        if (null != currentExecutor) {
            currentExecutor.close();
        }
    }

    @Override
    public final void reOpen(Connection connection) throws SQLException {
        this.connection = connection;
        ConcurrentMap<String, JdbcBatchStatementWrapper<RowData>>
                stringDynamicPreparedStmtConcurrentMap = cache.asMap();
        initCache(true);
        for (Map.Entry<String, JdbcBatchStatementWrapper<RowData>> entry :
                stringDynamicPreparedStmtConcurrentMap.entrySet()) {
            JdbcBatchStatementWrapper<RowData> value = entry.getValue();
            value.reOpen(connection);
            cache.put(entry.getKey(), value);
            currentExecutor = value;
        }
    }

    protected abstract RowData switchExecutorFromCache(RowData record) throws ExecutionException;

    protected void initCache(boolean isExpired) {
        CacheBuilder<String, JdbcBatchStatementWrapper<RowData>> cacheBuilder =
                CacheBuilder.newBuilder()
                        .maximumSize(cacheSize)
                        .removalListener(
                                notification -> {
                                    try {
                                        assert notification.getValue() != null;
                                        notification.getValue().close();
                                    } catch (SQLException e) {
                                        Log.error("", e);
                                    }
                                });
        if (isExpired) {
            cacheBuilder.expireAfterAccess(cacheDurationMin, TimeUnit.MINUTES);
        }
        this.cache = cacheBuilder.build();
    }

    public void clearStatementCache() {
        cache.invalidateAll();
    }
}
