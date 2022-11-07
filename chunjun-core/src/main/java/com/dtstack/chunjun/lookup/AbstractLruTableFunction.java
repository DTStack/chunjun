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
import com.dtstack.chunjun.lookup.cache.LRUSideCache;
import com.dtstack.chunjun.lookup.conf.LookupConf;
import com.dtstack.chunjun.util.ReflectionUtils;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.execution.SuppressRestartsException;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.FunctionContext;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.stream.Collectors;

/**
 * @author chuixue
 * @create 2021-04-09 14:40
 * @description
 */
public abstract class AbstractLruTableFunction extends AsyncTableFunction<RowData> {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractLruTableFunction.class);
    /** 指标 */
    protected transient Counter parseErrorRecords;
    /** 缓存 */
    protected AbstractSideCache sideCache;
    /** 维表配置 */
    protected LookupConf lookupConf;
    /** 运行环境 */
    private RuntimeContext runtimeContext;
    /** 数据类型转换器 */
    protected final AbstractRowConverter rowConverter;

    private static final int TIMEOUT_LOG_FLUSH_NUM = 10;
    private int timeOutNum = 0;

    public AbstractLruTableFunction(LookupConf lookupConf, AbstractRowConverter rowConverter) {
        this.lookupConf = lookupConf;
        this.rowConverter = rowConverter;
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);

        initCache();
        initMetric(context);

        Field field = FunctionContext.class.getDeclaredField("context");
        field.setAccessible(true);
        runtimeContext = (RuntimeContext) field.get(context);

        LOG.info("async dim table lookupOptions info: {} ", lookupConf.toString());
    }

    /** 初始化缓存 */
    @VisibleForTesting
    protected void initCache() {
        if (CacheType.NONE.name().equalsIgnoreCase(lookupConf.getCache())) {
            return;
        }

        if (CacheType.LRU.name().equalsIgnoreCase(lookupConf.getCache())) {
            sideCache = new LRUSideCache(lookupConf.getCacheSize(), lookupConf.getCacheTtl());
        } else {
            throw new RuntimeException("not support side cache with type:" + lookupConf.getCache());
        }

        sideCache.initCache();
    }

    /**
     * 初始化Metric
     *
     * @param context 上下文
     */
    @VisibleForTesting
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
            LOG.info(
                    "Async function call has timed out. input:{}, timeOutNum:{}", keys, timeOutNum);
        }
        timeOutNum++;

        if (timeOutNum > lookupConf.getErrorLimit()) {
            future.completeExceptionally(
                    new SuppressRestartsException(
                            new Throwable(
                                    String.format(
                                            "Async function call timedOutNum beyond limit. %s",
                                            lookupConf.getErrorLimit()))));
        } else {
            future.complete(Collections.EMPTY_LIST);
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
     * @param future 发送到下游
     * @param keys 关联数据
     */
    public void eval(CompletableFuture<Collection<RowData>> future, Object... keys) {
        try {
            preInvoke(future, keys);

            String cacheKey = buildCacheKey(keys);
            // 缓存判断
            if (isUseCache(cacheKey)) {
                invokeWithCache(cacheKey, future);
                return;
            }
            handleAsyncInvoke(future, keys);
        } catch (Exception e) {
            // todo 优化
            LOG.error(e.getMessage());
        }
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
                return;
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
        return Arrays.stream(keys).map(e -> String.valueOf(e)).collect(Collectors.joining("_"));
    }

    private ProcessingTimeService getProcessingTimeService() {
        try {
            Class<?>[] declaredClasses = RichAsyncFunction.class.getDeclaredClasses();

            Class richAsyncFunctionClass = null;
            for (Class cl : declaredClasses) {
                if (cl.getSimpleName().equals("RichAsyncFunctionRuntimeContext")) {
                    richAsyncFunctionClass = cl;
                }
            }

            Field runtimeContextField = richAsyncFunctionClass.getDeclaredField("runtimeContext");
            runtimeContextField.setAccessible(true);
            Object functionRuntimeContext = runtimeContextField.get(runtimeContext);

            Field streamingRuntimeContextField =
                    richAsyncFunctionClass.getDeclaredField("runtimeContext");
            runtimeContextField.setAccessible(true);
            StreamingRuntimeContext streamingRuntimeContext =
                    (StreamingRuntimeContext)
                            streamingRuntimeContextField.get(functionRuntimeContext);

            Field processingTimeServiceField =
                    StreamingRuntimeContext.class.getDeclaredField("processingTimeService");
            processingTimeServiceField.setAccessible(true);
            ProcessingTimeService processingTimeService =
                    (ProcessingTimeService) processingTimeServiceField.get(streamingRuntimeContext);
            return processingTimeService;
        } catch (IllegalAccessException | NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
    }

    protected ScheduledFuture<?> registerTimer(
            CompletableFuture<Collection<RowData>> future, Object... keys) {
        ProcessingTimeService processingTimeService = getProcessingTimeService();
        long timeoutTimestamp =
                lookupConf.getAsyncTimeout() + processingTimeService.getCurrentProcessingTime();
        return processingTimeService.registerTimer(
                timeoutTimestamp, timestamp -> timeout(future, keys));
    }

    protected void registerTimerAndAddToHandler(
            CompletableFuture<Collection<RowData>> future, Object... keys)
            throws InvocationTargetException, IllegalAccessException {
        ScheduledFuture<?> timeFuture = registerTimer(future, keys);
        // resultFuture 是ResultHandler 的实例
        Method setTimeoutTimer =
                ReflectionUtils.getDeclaredMethod(future, "setTimeoutTimer", ScheduledFuture.class);
        setTimeoutTimer.setAccessible(true);
        setTimeoutTimer.invoke(future, timeFuture);
    }

    /**
     * 发送异常
     *
     * @param future
     * @param e
     */
    protected void dealFillDataError(CompletableFuture<Collection<RowData>> future, Throwable e) {
        parseErrorRecords.inc();
        if (parseErrorRecords.getCount() > lookupConf.getErrorLimit()) {
            LOG.info("dealFillDataError", e);
            future.completeExceptionally(new SuppressRestartsException(e));
        } else {
            dealMissKey(future);
        }
    }

    /**
     * 资源释放
     *
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        super.close();
    }
}
