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

package com.dtstack.chunjun.connector.cassandra.lookup;

import com.dtstack.chunjun.connector.cassandra.config.CassandraCommonConfig;
import com.dtstack.chunjun.connector.cassandra.config.CassandraLookupConfig;
import com.dtstack.chunjun.connector.cassandra.util.CassandraService;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.enums.ECacheContentType;
import com.dtstack.chunjun.lookup.AbstractLruTableFunction;
import com.dtstack.chunjun.lookup.cache.CacheMissVal;
import com.dtstack.chunjun.lookup.cache.CacheObj;
import com.dtstack.chunjun.lookup.config.LookupConfig;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PagingIterable;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.dtstack.chunjun.connector.cassandra.util.CassandraService.quoteColumn;

@Slf4j
public class CassandraLruTableFunction extends AbstractLruTableFunction {

    private static final long serialVersionUID = -8674524807253535362L;

    private final CassandraLookupConfig cassandraLookupConfig;

    private transient Cluster cluster;

    private transient ListenableFuture<Session> session;

    private final String[] fieldNames;

    private final String[] keyNames;

    public CassandraLruTableFunction(
            LookupConfig lookupConfig,
            AbstractRowConverter<?, ?, ?, ?> rowConverter,
            String[] fieldNames,
            String[] keyNames) {
        super(lookupConfig, rowConverter);
        this.cassandraLookupConfig = (CassandraLookupConfig) lookupConfig;
        this.fieldNames = fieldNames;
        this.keyNames = keyNames;
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);

        cluster = CassandraService.cluster(cassandraLookupConfig.getCommonConfig());
        session = cluster.connectAsync();
    }

    @Override
    public void handleAsyncInvoke(CompletableFuture<Collection<RowData>> future, Object... keys) {
        String key = buildCacheKey(keys);

        if (StringUtils.isBlank(key)) {
            return;
        }

        CassandraCommonConfig commonConfig = cassandraLookupConfig.getCommonConfig();
        String keyspaces = commonConfig.getKeyspaces();
        String tableName = commonConfig.getTableName();

        List<String> quotedColumnNameList = new ArrayList<>();
        Arrays.stream(fieldNames).forEach(name -> quotedColumnNameList.add(quoteColumn(name)));
        Select select =
                QueryBuilder.select(quotedColumnNameList.toArray(new String[0]))
                        .from(keyspaces, tableName);

        for (int index = 0; index < keyNames.length; index++) {
            Clause eq = QueryBuilder.eq(quoteColumn(keyNames[index]), keys[index]);
            select.where(eq);
        }

        ListenableFuture<ResultSet> resultSetListenableFuture =
                Futures.transformAsync(session, session -> session.executeAsync(select));

        ListenableFuture<List<Row>> data =
                Futures.transform(
                        resultSetListenableFuture,
                        (Function<ResultSet, List<Row>>) PagingIterable::all);

        Futures.addCallback(
                data,
                new FutureCallback<List<Row>>() {
                    @Override
                    public void onSuccess(List<Row> rows) {
                        if (rows.size() > 0) {
                            List<Row> cacheContent = Lists.newArrayList();
                            List<RowData> rowList = Lists.newArrayList();
                            for (Row line : rows) {
                                try {
                                    RowData row = rowConverter.toInternalLookup(line);
                                    if (openCache()) {
                                        cacheContent.add(line);
                                    }
                                    rowList.add(row);
                                } catch (Exception e) {
                                    // todo 这里需要抽样打印
                                    log.error("error:{}\n data:{}", e.getMessage(), line);
                                }
                            }
                            future.complete(rowList);
                            if (openCache()) {
                                putCache(
                                        key,
                                        CacheObj.buildCacheObj(
                                                ECacheContentType.MultiLine, cacheContent));
                            }
                        } else {
                            dealMissKey(future);
                            if (openCache()) {
                                putCache(key, CacheMissVal.getMissKeyObj());
                            }
                            future.complete(Collections.emptyList());
                        }
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        log.error("Failed to query the data.", t);
                        cluster.closeAsync();
                        future.completeExceptionally(t);
                    }
                });
    }

    @Override
    public void close() {
        if (cluster != null && !cluster.isClosed()) {
            cluster.close();
        }

        if (session != null && !session.isCancelled()) {
            session.cancel(false);
        }
    }
}
