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

package com.dtstack.chunjun.cdc.handler;

import com.dtstack.chunjun.cdc.cache.Cache;
import com.dtstack.chunjun.cdc.config.CacheConfig;
import com.dtstack.chunjun.cdc.ddl.definition.TableIdentifier;
import com.dtstack.chunjun.util.GsonUtil;

import org.apache.flink.table.data.RowData;

import com.google.common.collect.Queues;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

@Slf4j
public abstract class CacheHandler implements Serializable {
    private static final long serialVersionUID = 6758334667931679889L;

    protected static final Gson GSON =
            GsonUtil.setTypeAdapter(
                    new GsonBuilder()
                            .setDateFormat("yyyy-MM-dd HH:mm:ss")
                            .disableHtmlEscaping()
                            .create());

    protected final CacheConfig cacheConfig;
    private final Map<TableIdentifier, Cache> cacheMap = new ConcurrentHashMap<>();
    private final Map<TableIdentifier, Queue<RowData>> temporaryQueueMap =
            new ConcurrentHashMap<>();
    private final List<TableIdentifier> blockedTableIdentifiers = new CopyOnWriteArrayList<>();
    private final List<TableIdentifier> unblockedTableIdentifiers = new CopyOnWriteArrayList<>();

    public CacheHandler(CacheConfig cacheConfig) {
        this.cacheConfig = cacheConfig;
    }

    public boolean contains(TableIdentifier tableIdentifier) {
        return cacheMap.containsKey(tableIdentifier);
    }

    public boolean isBlock(TableIdentifier tableIdentifier) {
        return blockedTableIdentifiers.contains(tableIdentifier);
    }

    public void open() throws Exception {
        Properties properties = cacheConfig.getProperties();
        init(properties);
    }

    public void close() throws Exception {
        shutdown();
    }

    public void addNewBlockCache(TableIdentifier tableIdentifier) {
        Cache cache = new Cache(cacheConfig.getCacheSize(), cacheConfig.getCacheTimeout());
        blockedTableIdentifiers.add(tableIdentifier);
        cacheMap.put(tableIdentifier, cache);
    }

    public void add(TableIdentifier tableIdentifier, RowData data) {
        Cache cache;
        if (cacheMap.containsKey(tableIdentifier)) {
            cache = cacheMap.get(tableIdentifier);
            cache.add(data);
        } else {
            cache = new Cache(cacheConfig.getCacheSize(), cacheConfig.getCacheTimeout());
            cache.add(data);
            unblockedTableIdentifiers.add(tableIdentifier);
            cacheMap.put(tableIdentifier, cache);
        }
        startCacheService(cache, tableIdentifier);
    }

    private void startCacheService(Cache cache, TableIdentifier tableIdentifier) {
        // 如果数据量超过限制，那么将数据下发。
        if (cache.backupIsFull()) {
            Queue<RowData> fromBackup = cache.allFromBackup();
            if (sendCache(fromBackup, tableIdentifier)) {
                cacheMap.get(tableIdentifier).cached();
            }
        }
    }

    public void block(TableIdentifier tableIdentifier) {
        blockedTableIdentifiers.add(tableIdentifier);
        unblockedTableIdentifiers.remove(tableIdentifier);
    }

    public void unblock(TableIdentifier tableIdentifier) {
        blockedTableIdentifiers.remove(tableIdentifier);
        unblockedTableIdentifiers.add(tableIdentifier);
    }

    public RowData get(TableIdentifier tableIdentifier) {
        // 如果已经从外部缓存中获取了数据，并且暂存在了内存中，那么从内存中获取数据
        Queue<RowData> temporaryQueue = temporaryQueueMap.get(tableIdentifier);
        if (null != temporaryQueue && !temporaryQueue.isEmpty()) {
            return temporaryQueue.peek();
        }

        Cache cache = cacheMap.get(tableIdentifier);

        // 先下发out 队列中的数据
        RowData fromOut = cache.oneFromOut();
        if (null != fromOut) {
            return fromOut;
        }

        if (cache.isCached()) {
            Queue<RowData> fromCache = fromCache(tableIdentifier);
            // 如果fromCache为空，说明外部缓存表中没有缓存数据
            if (fromCache.isEmpty()) {
                Queue<RowData> backup = Queues.newLinkedBlockingQueue(cache.allFromBackup());
                cache.uncached();
                if (!backup.isEmpty()) {
                    temporaryQueueMap.put(tableIdentifier, backup);
                    return temporaryQueueMap.get(tableIdentifier).peek();
                }
            }
            // 将查询出来的结果放入内存中缓存
            temporaryQueueMap.put(tableIdentifier, fromCache);
            return temporaryQueueMap.get(tableIdentifier).peek();
        }

        if (!cache.allFromBackup().isEmpty()) {
            cache.swapToOut();
        }

        return cache.oneFromOut();
    }

    public void remove(TableIdentifier tableIdentifier, RowData data) {
        Cache cache = cacheMap.get(tableIdentifier);
        cache.removeOut(data);
        Queue<RowData> dataQueue = temporaryQueueMap.get(tableIdentifier);
        if (dataQueue != null) {
            dataQueue.remove(data);
        }
    }

    public Cache getCache(TableIdentifier tableIdentifier) {
        return cacheMap.get(tableIdentifier);
    }

    public List<TableIdentifier> getBlockedTableIdentifiers() {
        return blockedTableIdentifiers;
    }

    public List<TableIdentifier> getUnblockedTableIdentifiers() {
        return unblockedTableIdentifiers;
    }

    public abstract void init(Properties properties) throws Exception;

    public abstract void shutdown() throws Exception;

    public abstract boolean sendCache(Collection<RowData> data, TableIdentifier tableIdentifier);

    public abstract Queue<RowData> fromCache(TableIdentifier tableIdentifier);

    public abstract void deleteCache(TableIdentifier tableIdentifier, String lsn, int lsnSequence);
}
