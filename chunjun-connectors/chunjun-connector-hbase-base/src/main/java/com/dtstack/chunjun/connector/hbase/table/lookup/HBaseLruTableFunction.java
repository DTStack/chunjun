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

package com.dtstack.chunjun.connector.hbase.table.lookup;

import com.dtstack.chunjun.connector.hbase.HBaseTableSchema;
import com.dtstack.chunjun.connector.hbase.config.HBaseConfig;
import com.dtstack.chunjun.connector.hbase.converter.HBaseSerde;
import com.dtstack.chunjun.connector.hbase.util.HBaseHelper;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.enums.ECacheContentType;
import com.dtstack.chunjun.factory.ChunJunThreadFactory;
import com.dtstack.chunjun.lookup.AbstractLruTableFunction;
import com.dtstack.chunjun.lookup.cache.CacheMissVal;
import com.dtstack.chunjun.lookup.cache.CacheObj;
import com.dtstack.chunjun.lookup.config.LookupConfig;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Slf4j
public class HBaseLruTableFunction extends AbstractLruTableFunction {
    private static final long serialVersionUID = 2297990960765338476L;

    private static final int DEFAULT_BOSS_THREADS = 1;
    private static final int DEFAULT_IO_THREADS = Runtime.getRuntime().availableProcessors() * 2;
    private static final int DEFAULT_POOL_SIZE = DEFAULT_IO_THREADS + DEFAULT_BOSS_THREADS;

    private transient Connection connection;
    private transient Table table;

    private transient ExecutorService executorService;

    private final HBaseTableSchema hbaseTableSchema;

    private transient HBaseSerde serde;

    private final HBaseConfig hBaseConfig;

    public HBaseLruTableFunction(
            LookupConfig lookupConfig,
            HBaseTableSchema hbaseTableSchema,
            HBaseConfig hBaseConfig,
            AbstractRowConverter rowConverter) {
        super(lookupConfig, rowConverter);
        this.hBaseConfig = hBaseConfig;
        this.hbaseTableSchema = hbaseTableSchema;
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        this.serde = new HBaseSerde(hbaseTableSchema, hBaseConfig);
        this.executorService =
                new ThreadPoolExecutor(
                        DEFAULT_POOL_SIZE,
                        DEFAULT_POOL_SIZE,
                        0L,
                        TimeUnit.MILLISECONDS,
                        new LinkedBlockingQueue<>(),
                        new ChunJunThreadFactory("hbase-async"));

        this.connection = HBaseHelper.getHbaseConnection(hBaseConfig, null, null);
        this.table = connection.getTable(TableName.valueOf(hbaseTableSchema.getTableName()));
    }

    @Override
    public void handleAsyncInvoke(
            CompletableFuture<Collection<RowData>> future, Object... rowKeys) {

        executorService.execute(
                new Runnable() {
                    @Override
                    public void run() {
                        Object rowKey = rowKeys[0];
                        byte[] key = serde.getRowKey(rowKey);
                        String keyStr = new String(key);
                        try {
                            Get get = new Get(key);
                            Result result = table.get(get);
                            if (!result.isEmpty()) {
                                RowData data = serde.convertToNewRow(result);
                                if (openCache()) {
                                    sideCache.putCache(
                                            keyStr,
                                            CacheObj.buildCacheObj(
                                                    ECacheContentType.MultiLine,
                                                    Collections.singletonList(data)));
                                }
                                future.complete(Collections.singletonList(data));
                            } else {
                                dealMissKey(future);
                                if (openCache()) {
                                    sideCache.putCache(keyStr, CacheMissVal.getMissKeyObj());
                                }
                            }
                        } catch (IOException e) {
                            log.error("record:" + keyStr);
                            log.error("get side record exception:" + e);
                            future.complete(Collections.emptyList());
                        }
                    }
                });
    }

    @Override
    public void close() throws Exception {
        table.close();
        connection.close();
        executorService.shutdown();
        super.close();
    }
}
