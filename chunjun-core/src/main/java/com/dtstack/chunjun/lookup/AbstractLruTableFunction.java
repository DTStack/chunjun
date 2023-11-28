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

package com.dtstack.chunjun.lookup;

import com.dtstack.chunjun.constants.Metrics;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.enums.CacheType;
import com.dtstack.chunjun.enums.ECacheContentType;
import com.dtstack.chunjun.lookup.cache.AbstractSideCache;
import com.dtstack.chunjun.lookup.cache.CacheObj;
import com.dtstack.chunjun.lookup.cache.LRUCache;
import com.dtstack.chunjun.lookup.config.LookupConfig;

import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.execution.SuppressRestartsException;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.AsyncLookupFunction;
import org.apache.flink.table.functions.FunctionContext;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

@Slf4j
public abstract class AbstractLruTableFunction extends AsyncLookupFunction {

    private static final long serialVersionUID = 8054577160378024212L;
    /** 指标 */
    protected transient Counter parseErrorRecords;
    /** 缓存 */
    protected AbstractSideCache sideCache;
    /** 维表配置 */
    protected LookupConfig lookupConfig;
    /** 数据类型转换器 */
    protected final AbstractRowConverter rowConverter;

    private static final int TIMEOUT_LOG_FLUSH_NUM = 10;
    private int timeOutNum = 0;

    public AbstractLruTableFunction(LookupConfig lookupConfig, AbstractRowConverter rowConverter) {
        this.lookupConfig = lookupConfig;
        this.rowConverter = rowConverter;
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);

        initCache();
        initMetric(context);

        log.info("async dim table lookupOptions info: {} ", lookupConfig.toString());
    }

    /** 初始化缓存 */
    protected void initCache() {
        if (CacheType.NONE.name().equalsIgnoreCase(lookupConfig.getCache())) {
            return;
        }

        if (CacheType.LRU.name().equalsIgnoreCase(lookupConfig.getCache())) {
            sideCache = new LRUCache(lookupConfig.getCacheSize(), lookupConfig.getCacheTtl());
        } else {
            throw new RuntimeException(
                    "not support side cache with type:" + lookupConfig.getCache());
        }

        sideCache.initCache();
    }

    /**
     * 初始化Metric
     *
     * @param context 上下文
     */
    protected void initMetric(FunctionContext context) {
        parseErrorRecords = context.getMetricGroup().counter(Metrics.NUM_SIDE_PARSE_ERROR_RECORDS);
    }

    /**
     * 通过key得到缓存数据
     *
     * @param key
     * @return
     */
    protected CacheObj getFromCache(String key) {
        return sideCache.getFromCache(key);
    }

    /**
     * 数据放入缓存
     *
     * @param key
     * @param value
     */
    protected void putCache(String key, CacheObj value) {
        sideCache.putCache(key, value);
    }

    /**
     * 是否开启缓存
     *
     * @return
     */
    protected boolean openCache() {
        return sideCache != null;
    }

    /**
     * 如果缓存获取不到，直接返回空即可，无需判别左/内连接
     *
     * @param future
     */
    public void dealMissKey(CompletableFuture<Collection<RowData>> future) {
        try {
            future.complete(Collections.emptyList());
        } catch (Exception e) {
            dealFillDataError(future, e);
        }
    }

    /**
     * 判断是否需要放入缓存
     *
     * @param key
     * @param missKeyObj
     */
    protected void dealCacheData(String key, CacheObj missKeyObj) {
        if (openCache()) {
            putCache(key, missKeyObj);
        }
    }

    // todo 无法设置超时
    public void timeout(CompletableFuture<Collection<RowData>> future, Object... keys) {
        if (timeOutNum % TIMEOUT_LOG_FLUSH_NUM == 0) {
            log.info(
                    "Async function call has timed out. input:{}, timeOutNum:{}", keys, timeOutNum);
        }
        timeOutNum++;

        if (timeOutNum > lookupConfig.getErrorLimit()) {
            future.completeExceptionally(
                    new SuppressRestartsException(
                            new Throwable(
                                    String.format(
                                            "Async function call timedOutNum beyond limit. %s",
                                            lookupConfig.getErrorLimit()))));
        } else {
            future.complete(Collections.emptyList());
        }
    }

    /**
     * pre invoke
     *
     * @param future
     * @param keys
     * @throws InvocationTargetException
     * @throws IllegalAccessException
     */
    protected void preInvoke(CompletableFuture<Collection<RowData>> future, Object... keys)
            throws InvocationTargetException, IllegalAccessException {
        // todo 超时回掉
        // registerTimerAndAddToHandler(future, keys);
    }

    /**
     * 异步查询数据
     *
     * @param keyRow 关联数据
     */
    @Override
    public CompletableFuture<Collection<RowData>> asyncLookup(RowData keyRow) {
        CompletableFuture<Collection<RowData>> lookupFuture = new CompletableFuture<>();
        try {
            preInvoke(lookupFuture, keyRow);

            String cacheKey = buildCacheKey(keyRow);
            // 缓存判断
            if (isUseCache(cacheKey)) {
                invokeWithCache(cacheKey, lookupFuture);
                return lookupFuture;
            }
            handleAsyncInvoke(lookupFuture, keyRow);
        } catch (Exception e) {
            // todo 优化
            log.error(e.getMessage());
        }
        return lookupFuture;
    }

    /**
     * 判断缓存是否存在
     *
     * @param cacheKey 缓存健
     * @return
     */
    protected boolean isUseCache(String cacheKey) {
        return openCache() && getFromCache(cacheKey) != null;
    }

    /**
     * 从缓存中获取数据
     *
     * @param cacheKey 缓存健
     * @param future
     */
    private void invokeWithCache(String cacheKey, CompletableFuture<Collection<RowData>> future) {
        if (openCache()) {
            CacheObj val = getFromCache(cacheKey);
            if (val != null) {
                if (ECacheContentType.MissVal == val.getType()) {
                    dealMissKey(future);
                    return;
                } else if (ECacheContentType.SingleLine == val.getType()) {
                    try {
                        RowData row = rowConverter.toInternalLookup(val.getContent());
                        future.complete(Collections.singleton(row));
                    } catch (Exception e) {
                        dealFillDataError(future, e);
                    }
                } else if (ECacheContentType.MultiLine == val.getType()) {
                    try {
                        List<RowData> rowList = Lists.newArrayList();
                        for (Object one : (List) val.getContent()) {
                            RowData row = rowConverter.toInternalLookup(one);
                            rowList.add(row);
                        }
                        future.complete(rowList);
                    } catch (Exception e) {
                        dealFillDataError(future, e);
                    }
                } else {
                    future.completeExceptionally(
                            new RuntimeException("not support cache obj type " + val.getType()));
                }
            }
        }
    }

    /**
     * 请求数据库获取数据
     *
     * @param keys 关联字段数据
     * @param future
     * @throws Exception
     */
    public abstract void handleAsyncInvoke(
            CompletableFuture<Collection<RowData>> future, Object... keys) throws Exception;

    /**
     * 构建缓存key值
     *
     * @param keys
     * @return
     */
    public String buildCacheKey(Object... keys) {
        if (keys != null && keys.length == 1 && keys[0] instanceof GenericRowData) {
            GenericRowData rowData = (GenericRowData) keys[0];
            int[] keyIndexes = new int[rowData.getArity()];
            for (int i = 0; i < rowData.getArity(); i++) {
                keyIndexes[i] = i;
            }
            return Arrays.stream(keyIndexes)
                    .mapToObj(index -> String.valueOf(rowData.getField(index)))
                    .collect(Collectors.joining("_"));
        }
        return Arrays.stream(keys).map(String::valueOf).collect(Collectors.joining("_"));
    }

    /**
     * 发送异常
     *
     * @param future
     * @param e
     */
    protected void dealFillDataError(CompletableFuture<Collection<RowData>> future, Throwable e) {
        parseErrorRecords.inc();
        if (parseErrorRecords.getCount() > lookupConfig.getErrorLimit()) {
            log.info("dealFillDataError", e);
            future.completeExceptionally(new SuppressRestartsException(e));
        } else {
            dealMissKey(future);
        }
    }
}
