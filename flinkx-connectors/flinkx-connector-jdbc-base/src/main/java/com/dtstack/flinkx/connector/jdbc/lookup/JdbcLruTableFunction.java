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

package com.dtstack.flinkx.connector.jdbc.lookup;

import org.apache.flink.connector.jdbc.internal.converter.JdbcRowConverter;
import org.apache.flink.connector.jdbc.internal.options.JdbcOptions;
import org.apache.flink.runtime.execution.SuppressRestartsException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.types.logical.RowType;

import com.dtstack.flinkx.connector.jdbc.options.JdbcLookupOptions;
import com.dtstack.flinkx.enums.ECacheContentType;
import com.dtstack.flinkx.exception.ExceptionTrace;
import com.dtstack.flinkx.factory.DTThreadFactory;
import com.dtstack.flinkx.lookup.AbstractLruTableFunction;
import com.dtstack.flinkx.lookup.cache.CacheMissVal;
import com.dtstack.flinkx.lookup.cache.CacheObj;
import com.dtstack.flinkx.lookup.options.LookupOptions;
import com.dtstack.flinkx.util.DateUtil;
import com.google.common.collect.Lists;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.SQLClient;
import io.vertx.ext.sql.SQLConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import static com.dtstack.flinkx.connector.jdbc.constants.JdbcLookUpConstants.DEFAULT_DB_CONN_POOL_SIZE;
import static com.dtstack.flinkx.connector.jdbc.constants.JdbcLookUpConstants.DEFAULT_VERTX_EVENT_LOOP_POOL_SIZE;
import static com.dtstack.flinkx.connector.jdbc.constants.JdbcLookUpConstants.MAX_DB_CONN_POOL_SIZE_LIMIT;
import static com.dtstack.flinkx.connector.jdbc.constants.JdbcLookUpConstants.MAX_TASK_QUEUE_SIZE;
/**
 * @author chuixue
 * @create 2021-04-10 21:15
 * @description
 **/
abstract public class JdbcLruTableFunction extends AbstractLruTableFunction {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(JdbcLruTableFunction.class);
    /**  */
    private AtomicBoolean connectionStatus = new AtomicBoolean(true);
    /**  */
    private transient ThreadPoolExecutor executor;
    /**  */
    private transient SQLClient rdbSqlClient;
    /**  */
    private final String query;
    /**  */
    private final JdbcRowConverter jdbcRowConverter;

    private final int errorLogPrintNum = 3;

    protected JdbcOptions options ;

    protected int asyncPoolSize;

    public JdbcLruTableFunction(
            JdbcOptions options,
            LookupOptions lookupOptions,
            String[] fieldNames,
            String[] keyNames,
            RowType rowType) {
        super(lookupOptions);
        this.asyncPoolSize = ((JdbcLookupOptions) lookupOptions).getAsyncPoolSize();
        this.options = options;
        this.query =
                options.getDialect()
                        .getSelectFromStatement(options.getTableName(), fieldNames, keyNames);
        this.jdbcRowConverter = options.getDialect().getRowConverter(rowType);
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);

        int defaultAsyncPoolSize = Math.min(MAX_DB_CONN_POOL_SIZE_LIMIT.defaultValue(), DEFAULT_DB_CONN_POOL_SIZE.defaultValue());
        asyncPoolSize = asyncPoolSize > 0 ? asyncPoolSize : defaultAsyncPoolSize;

        VertxOptions vertxOptions = new VertxOptions();
        JsonObject jdbcConfig = buildJdbcConfig();
        System.setProperty("vertx.disableFileCPResolving", "true");
        vertxOptions
                .setEventLoopPoolSize(DEFAULT_VERTX_EVENT_LOOP_POOL_SIZE.defaultValue())
                .setWorkerPoolSize(asyncPoolSize)
                .setFileResolverCachingEnabled(false);

        this.rdbSqlClient = JDBCClient.createNonShared(Vertx.vertx(vertxOptions), jdbcConfig);

        executor = new ThreadPoolExecutor(
                MAX_DB_CONN_POOL_SIZE_LIMIT.defaultValue(),
                MAX_DB_CONN_POOL_SIZE_LIMIT.defaultValue(),
                0,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(MAX_TASK_QUEUE_SIZE.defaultValue()),
                new DTThreadFactory("rdbAsyncExec"),
                new ThreadPoolExecutor.CallerRunsPolicy());
        LOG.info("async dim table JdbcOptions info: {} ", options.toString());
    }

    @Override
    public void handleAsyncInvoke(
            CompletableFuture<Collection<RowData>> future, Object... keys) throws Exception {
        AtomicLong networkLogCounter = new AtomicLong(0L);
        //network is unhealthy
        while (!connectionStatus.get()) {
            if (networkLogCounter.getAndIncrement() % 1000 == 0) {
                LOG.info("network unhealthy to block task");
            }
            Thread.sleep(100);
        }

        executor.execute(() -> connectWithRetry(
                future,
                rdbSqlClient,
                Stream.of(keys)
                        .map(this::convertDataType).toArray(Object[]::new)));
    }

    private Object convertDataType(Object val) {
        if (val == null) {
            // OK
        } else if (val instanceof Number && !(val instanceof BigDecimal)) {
            // OK
        } else if (val instanceof Boolean) {
            // OK
        } else if (val instanceof String) {
            // OK
        } else if (val instanceof Character) {
            // OK
        } else if (val instanceof CharSequence) {

        } else if (val instanceof JsonObject) {

        } else if (val instanceof JsonArray) {

        } else if (val instanceof Map) {

        } else if (val instanceof List) {

        } else if (val instanceof byte[]) {

        } else if (val instanceof Instant) {

        } else if (val instanceof Timestamp) {
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
            CompletableFuture<Collection<RowData>> future,
            SQLClient rdbSqlClient,
            Object... keys) {
        AtomicLong failCounter = new AtomicLong(0);
        AtomicBoolean finishFlag = new AtomicBoolean(false);
        while (!finishFlag.get()) {
            try {
                CountDownLatch latch = new CountDownLatch(1);
                asyncQueryData(
                        future,
                        rdbSqlClient,
                        failCounter,
                        finishFlag,
                        latch,
                        keys);
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    LOG.error("", e);
                }
            } catch (Exception e) {
                //数据源队列溢出情况
                connectionStatus.set(false);
            }
            if (!finishFlag.get()) {
                try {
                    Thread.sleep(3000);
                } catch (Exception e) {
                    LOG.error("", e);
                }
            }
        }
    }

    /**
     * 执行异步查询
     *
     * @param future
     * @param rdbSqlClient 数据库客户端
     * @param failCounter  失败次数
     * @param finishFlag   完成标识
     * @param latch        同步标识
     * @param keys         关联字段值
     */
    protected void asyncQueryData(CompletableFuture<Collection<RowData>> future,
                                  SQLClient rdbSqlClient,
                                  AtomicLong failCounter,
                                  AtomicBoolean finishFlag,
                                  CountDownLatch latch,
                                  Object... keys) {
        doAsyncQueryData(
                future,
                rdbSqlClient,
                failCounter,
                finishFlag,
                latch,
                keys);
    }

    final protected void doAsyncQueryData(
            CompletableFuture<Collection<RowData>> future,
            SQLClient rdbSqlClient,
            AtomicLong failCounter,
            AtomicBoolean finishFlag,
            CountDownLatch latch,
            Object... keys) {
        rdbSqlClient.getConnection(conn -> {
            try {
                String errorMsg;
                Integer retryMaxNum = lookupOptions.getMaxRetryTimes();
                int logPrintTime = retryMaxNum / errorLogPrintNum == 0 ?
                        retryMaxNum : retryMaxNum / errorLogPrintNum;
                if (conn.failed()) {
                    connectionStatus.set(false);
                    errorMsg = ExceptionTrace.traceOriginalCause(conn.cause());
                    if (failCounter.getAndIncrement() % logPrintTime == 0) {
                        LOG.error("getConnection error. cause by " + errorMsg);
                    }
                    LOG.error(String.format("retry ... current time [%s]", failCounter.get()));
                    if (failCounter.get() >= retryMaxNum) {
                        future.completeExceptionally(
                                new SuppressRestartsException(
                                        new Throwable(
                                                ExceptionTrace.traceOriginalCause(conn.cause())
                                        )
                                )
                        );
                        finishFlag.set(true);
                    }
                    return;
                }
                connectionStatus.set(true);
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
     * @param keys       关联健值
     */
    private void handleQuery(SQLConnection connection, CompletableFuture<Collection<RowData>> future, Object... keys) {
        String cacheKey = buildCacheKey(keys);
        JsonArray params = new JsonArray();
        Stream.of(keys).forEach(params::add);
        connection.queryWithParams(query, params, rs -> {
            try {
                if (rs.failed()) {
                    LOG.error(
                            String.format("\nget data with sql [%s] failed! \ncause: [%s]",
                                    keys,
                                    rs.cause().getMessage()
                            )
                    );
                    dealFillDataError(future, rs.cause());
                    return;
                }

                List<JsonArray> cacheContent = Lists.newArrayList();
                int resultSize = rs.result().getResults().size();
                if (resultSize > 0) {
                    List<RowData> rowList = Lists.newArrayList();

                    for (JsonArray line : rs.result().getResults()) {
                        try {
                            RowData row = fillData(line);
                            if (openCache()) {
                                cacheContent.add(line);
                            }
                            rowList.add(row);
                        }catch (Exception e) {
                            LOG.error(e.getMessage() + ":" + line);
                        }
                    }

                    if (openCache()) {
                        putCache(
                                cacheKey,
                                CacheObj.buildCacheObj(ECacheContentType.MultiLine, cacheContent));
                    }
                    future.complete(rowList);
                } else {
                    dealMissKey(future);
                    if (openCache()) {
                        putCache(cacheKey, CacheMissVal.getMissKeyObj());
                    }
                }
            } finally {
                // and close the connection
                connection.close(done -> {
                    if (done.failed()) {
                        throw new RuntimeException(done.cause());
                    }
                });
            }
        });
    }

    @Override
    protected RowData fillData(
            Object sideInput) throws SQLException {
        return jdbcRowConverter.toInternal((JsonArray) sideInput);
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

    }

    /**
     * get jdbc connection
     * @return
     */
    abstract public JsonObject buildJdbcConfig() ;
}
