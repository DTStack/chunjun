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

package com.dtstack.chunjun.connector.jdbc.lookup;

import com.dtstack.chunjun.connector.jdbc.config.JdbcConfig;
import com.dtstack.chunjun.connector.jdbc.config.JdbcLookupConfig;
import com.dtstack.chunjun.connector.jdbc.dialect.JdbcDialect;
import com.dtstack.chunjun.enums.ECacheContentType;
import com.dtstack.chunjun.factory.ChunJunThreadFactory;
import com.dtstack.chunjun.lookup.AbstractLruTableFunction;
import com.dtstack.chunjun.lookup.cache.CacheMissVal;
import com.dtstack.chunjun.lookup.cache.CacheObj;
import com.dtstack.chunjun.lookup.config.LookupConfig;
import com.dtstack.chunjun.throwable.NoRestartException;
import com.dtstack.chunjun.util.DateUtil;
import com.dtstack.chunjun.util.JsonUtil;
import com.dtstack.chunjun.util.ThreadUtil;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.types.logical.RowType;

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.SQLClient;
import io.vertx.ext.sql.SQLConnection;
import lombok.extern.slf4j.Slf4j;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import static com.dtstack.chunjun.connector.jdbc.options.JdbcLookupOptions.DEFAULT_DB_CONN_POOL_SIZE;
import static com.dtstack.chunjun.connector.jdbc.options.JdbcLookupOptions.DEFAULT_VERTX_EVENT_LOOP_POOL_SIZE;
import static com.dtstack.chunjun.connector.jdbc.options.JdbcLookupOptions.DRUID_PREFIX;
import static com.dtstack.chunjun.connector.jdbc.options.JdbcLookupOptions.DT_PROVIDER_CLASS;
import static com.dtstack.chunjun.connector.jdbc.options.JdbcLookupOptions.ERRORLOG_PRINTNUM;
import static com.dtstack.chunjun.connector.jdbc.options.JdbcLookupOptions.MAX_DB_CONN_POOL_SIZE_LIMIT;
import static com.dtstack.chunjun.connector.jdbc.options.JdbcLookupOptions.MAX_TASK_QUEUE_SIZE;

@Slf4j
public class JdbcLruTableFunction extends AbstractLruTableFunction {

    private static final long serialVersionUID = 6913010430516007916L;

    /** when network is unhealthy block query */
    private final AtomicBoolean connectionStatus = new AtomicBoolean(true);
    /** select sql */
    private final String query;
    /** jdbc Dialect */
    private final JdbcDialect jdbcDialect;
    /** jdbc conf */
    private final JdbcConfig jdbcConfig;
    /** vertx async pool size */
    protected int asyncPoolSize;
    /** query data thread */
    private transient ThreadPoolExecutor executor;
    /** vertx */
    private transient Vertx vertx;
    /** rdb client */
    private transient SQLClient rdbSqlClient;

    public JdbcLruTableFunction(
            JdbcConfig jdbcConfig,
            JdbcDialect jdbcDialect,
            LookupConfig lookupConfig,
            String[] fieldNames,
            String[] keyNames,
            RowType rowType) {
        super(lookupConfig, jdbcDialect.getRowConverter(rowType));
        this.jdbcConfig = jdbcConfig;
        this.jdbcDialect = jdbcDialect;
        this.asyncPoolSize = ((JdbcLookupConfig) lookupConfig).getAsyncPoolSize();
        this.query =
                jdbcDialect.getSelectFromStatement(
                        jdbcConfig.getSchema(), jdbcConfig.getTable(), fieldNames, keyNames);
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);

        int defaultAsyncPoolSize =
                Math.min(
                        MAX_DB_CONN_POOL_SIZE_LIMIT.defaultValue(),
                        DEFAULT_DB_CONN_POOL_SIZE.defaultValue());
        asyncPoolSize = asyncPoolSize > 0 ? asyncPoolSize : defaultAsyncPoolSize;

        VertxOptions vertxOptions = new VertxOptions();
        JsonObject jdbcConfig =
                createJdbcConfig(((JdbcLookupConfig) lookupConfig).getDruidConfig());
        System.setProperty("vertx.disableFileCPResolving", "true");
        vertxOptions
                .setEventLoopPoolSize(DEFAULT_VERTX_EVENT_LOOP_POOL_SIZE.defaultValue())
                .setWorkerPoolSize(asyncPoolSize)
                .setFileResolverCachingEnabled(false);

        this.vertx = Vertx.vertx(vertxOptions);
        this.rdbSqlClient = JDBCClient.createNonShared(vertx, jdbcConfig);

        executor =
                new ThreadPoolExecutor(
                        MAX_DB_CONN_POOL_SIZE_LIMIT.defaultValue(),
                        MAX_DB_CONN_POOL_SIZE_LIMIT.defaultValue(),
                        0,
                        TimeUnit.MILLISECONDS,
                        new LinkedBlockingQueue<>(MAX_TASK_QUEUE_SIZE.defaultValue()),
                        new ChunJunThreadFactory("rdbAsyncExec"),
                        new ThreadPoolExecutor.CallerRunsPolicy());
        // 隐藏日志中明文密码
        log.info("async dim table JdbcOptions info: {} ", JsonUtil.toPrintJson(jdbcConfig));
    }

    @Override
    public void handleAsyncInvoke(CompletableFuture<Collection<RowData>> future, Object... keys)
            throws Exception {
        AtomicLong networkLogCounter = new AtomicLong(0L);
        // network is unhealthy
        while (!connectionStatus.get()) {
            if (networkLogCounter.getAndIncrement() % 1000 == 0) {
                log.info("network unhealthy to block task");
            }
            Thread.sleep(100);
        }

        List<Object> keyList = new ArrayList<>();
        for (Object key : keys) {
            if (key instanceof GenericRowData) {
                GenericRowData genericRowData = (GenericRowData) key;
                for (int i = 0; i < genericRowData.getArity(); i++) {
                    keyList.add(this.convertDataType(genericRowData.getField(i)));
                }
            } else {
                keyList.add(this.convertDataType(key));
            }
        }

        executor.execute(() -> connectWithRetry(future, rdbSqlClient, keyList.toArray()));
    }

    private Object convertDataType(Object val) {
        if (val instanceof Timestamp) {
            val = DateUtil.timestampToString((Timestamp) val);
        } else if (val instanceof java.util.Date) {
            val = DateUtil.dateToString((java.sql.Date) val);
        } else {
            val = val.toString();
        }
        return val;
    }

    /**
     * @param future
     * @param rdbSqlClient 数据库客户端
     * @param keys 关联字段值
     */
    private void connectWithRetry(
            CompletableFuture<Collection<RowData>> future, SQLClient rdbSqlClient, Object... keys) {
        AtomicLong failCounter = new AtomicLong(0);
        AtomicBoolean finishFlag = new AtomicBoolean(false);
        while (!finishFlag.get()) {
            try {
                CountDownLatch latch = new CountDownLatch(1);
                asyncQueryData(future, rdbSqlClient, failCounter, finishFlag, latch, keys);
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    log.error("", e);
                }
            } catch (Exception e) {
                // 数据源队列溢出情况
                connectionStatus.set(false);
            }
            if (!finishFlag.get()) {
                ThreadUtil.sleepSeconds(ThreadUtil.DEFAULT_SLEEP_TIME);
            }
        }
    }

    /**
     * 执行异步查询
     *
     * @param future
     * @param rdbSqlClient 数据库客户端
     * @param failCounter 失败次数
     * @param finishFlag 完成标识
     * @param latch 同步标识
     * @param keys 关联字段值
     */
    protected void asyncQueryData(
            CompletableFuture<Collection<RowData>> future,
            SQLClient rdbSqlClient,
            AtomicLong failCounter,
            AtomicBoolean finishFlag,
            CountDownLatch latch,
            Object... keys) {
        doAsyncQueryData(future, rdbSqlClient, failCounter, finishFlag, latch, keys);
    }

    protected final void doAsyncQueryData(
            CompletableFuture<Collection<RowData>> future,
            SQLClient rdbSqlClient,
            AtomicLong failCounter,
            AtomicBoolean finishFlag,
            CountDownLatch latch,
            Object... keys) {
        rdbSqlClient.getConnection(
                conn -> {
                    try {
                        Integer retryMaxNum = lookupConfig.getMaxRetryTimes();
                        int logPrintTime =
                                retryMaxNum / ERRORLOG_PRINTNUM.defaultValue() == 0
                                        ? retryMaxNum
                                        : retryMaxNum / ERRORLOG_PRINTNUM.defaultValue();
                        if (conn.failed()) {
                            connectionStatus.set(false);
                            if (failCounter.getAndIncrement() % logPrintTime == 0) {
                                log.error("getConnection error. ", conn.cause());
                            }
                            log.error(
                                    String.format(
                                            "retry ... current time [%s]", failCounter.get()));
                            if (failCounter.get() >= retryMaxNum) {
                                future.completeExceptionally(new NoRestartException(conn.cause()));
                                finishFlag.set(true);
                            }
                            return;
                        }
                        connectionStatus.set(true);
                        // todo
                        // registerTimerAndAddToHandler(future, keys);

                        handleQuery(conn.result(), future, keys);
                        finishFlag.set(true);
                    } catch (Exception e) {
                        dealFillDataError(future, e);
                    } finally {
                        latch.countDown();
                    }
                });
    }

    /**
     * 执行异步查询
     *
     * @param connection 连接
     * @param future
     * @param keys 关联健值
     */
    private void handleQuery(
            SQLConnection connection,
            CompletableFuture<Collection<RowData>> future,
            Object... keys) {
        String cacheKey = buildCacheKey(keys);
        JsonArray params = new JsonArray();
        Stream.of(keys).forEach(params::add);
        connection.queryWithParams(
                query,
                params,
                rs -> {
                    try {
                        if (rs.failed()) {
                            String msg =
                                    String.format(
                                            "\nget data with sql [%s],data [%s] failed! \ncause: [%s]",
                                            query, Arrays.toString(keys), rs.cause().getMessage());
                            log.error(msg);
                            future.completeExceptionally(new SQLException(msg));
                            return;
                        }

                        List<JsonArray> cacheContent = new ArrayList<>();
                        int resultSize = rs.result().getResults().size();
                        if (resultSize > 0) {
                            List<RowData> rowList = new ArrayList<>();

                            for (JsonArray line : rs.result().getResults()) {
                                try {
                                    RowData row = rowConverter.toInternalLookup(line);
                                    if (openCache()) {
                                        cacheContent.add(line);
                                    }
                                    rowList.add(row);
                                } catch (Exception e) {
                                    // todo 这里需要抽样打印
                                    log.error(
                                            "error:{} \n sql:{} \n data:{}",
                                            e.getMessage(),
                                            jdbcConfig.getQuerySql(),
                                            line);
                                }
                            }

                            dealCacheData(
                                    cacheKey,
                                    CacheObj.buildCacheObj(
                                            ECacheContentType.MultiLine, cacheContent));
                            future.complete(rowList);
                        } else {
                            dealMissKey(future);
                            dealCacheData(cacheKey, CacheMissVal.getMissKeyObj());
                        }
                    } finally {
                        // and close the connection
                        connection.close(
                                done -> {
                                    if (done.failed()) {
                                        log.error("sql connection close failed! ", done.cause());
                                    }
                                });
                    }
                });
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (rdbSqlClient != null) {
            rdbSqlClient.close();
        }

        if (executor != null) {
            executor.shutdown();
        }
        // 关闭异步连接vertx事件循环线程，因为vertx使用的是非守护线程
        if (Objects.nonNull(vertx)) {
            vertx.close(
                    done -> {
                        if (done.failed()) {
                            log.error("vert.x close error. cause by {}", done.cause().getMessage());
                        }
                    });
        }
    }

    /**
     * get jdbc connection
     *
     * @return
     */
    public JsonObject createJdbcConfig(Map<String, Object> druidConfMap) {
        JsonObject clientConfig = new JsonObject(druidConfMap);
        clientConfig
                .put(DRUID_PREFIX + "url", jdbcConfig.getJdbcUrl())
                .put(DRUID_PREFIX + "username", jdbcConfig.getUsername())
                .put(DRUID_PREFIX + "password", jdbcConfig.getPassword())
                .put(
                        DRUID_PREFIX + "driverClassName",
                        jdbcDialect.defaultDriverName().orElseThrow(IllegalArgumentException::new))
                .put("provider_class", DT_PROVIDER_CLASS.defaultValue())
                .put(DRUID_PREFIX + "maxActive", asyncPoolSize);

        return clientConfig;
    }
}
