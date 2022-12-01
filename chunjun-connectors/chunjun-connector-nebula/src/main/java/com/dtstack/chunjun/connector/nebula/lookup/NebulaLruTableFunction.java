package com.dtstack.chunjun.connector.nebula.lookup;
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

import com.dtstack.chunjun.connector.nebula.client.NebulaClientFactory;
import com.dtstack.chunjun.connector.nebula.client.NebulaSession;
import com.dtstack.chunjun.connector.nebula.conf.NebulaConf;
import com.dtstack.chunjun.connector.nebula.lookup.ngql.LookupNGQLBuilder;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.enums.ECacheContentType;
import com.dtstack.chunjun.factory.ChunJunThreadFactory;
import com.dtstack.chunjun.lookup.AbstractLruTableFunction;
import com.dtstack.chunjun.lookup.cache.CacheMissVal;
import com.dtstack.chunjun.lookup.cache.CacheObj;
import com.dtstack.chunjun.lookup.config.LookupConfig;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;

import com.google.common.collect.Lists;
import com.vesoft.nebula.client.graph.data.ResultSet;
import com.vesoft.nebula.client.graph.data.ValueWrapper;
import com.vesoft.nebula.client.graph.exception.IOErrorException;
import com.vesoft.nebula.client.storage.data.BaseTableRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author: gaoasi
 * @email: aschaser@163.com
 * @date: 2022/11/8 4:34 下午
 */
public class NebulaLruTableFunction extends AbstractLruTableFunction {
    private static final Logger LOG = LoggerFactory.getLogger(NebulaLruTableFunction.class);
    private static final long serialVersionUID = 1L;

    private final NebulaConf nebulaConf;

    private final String[] keyNames;

    private final String[] fieldNames;
    private NebulaSession nebulaSession;

    /** query data thread */
    private transient ThreadPoolExecutor executor;

    public NebulaLruTableFunction(
            NebulaConf nebulaConf,
            LookupConfig lookupConf,
            String[] fieldNames,
            String[] keyNames,
            AbstractRowConverter rowConverter) {
        super(lookupConf, rowConverter);
        this.fieldNames = fieldNames;
        this.nebulaConf = nebulaConf;
        this.keyNames = keyNames;
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        nebulaSession = NebulaClientFactory.createNebulaSession(nebulaConf);
        nebulaSession.init();
        executor =
                new ThreadPoolExecutor(
                        nebulaConf.getMaxConnsSize(),
                        nebulaConf.getMaxConnsSize(),
                        0,
                        TimeUnit.MILLISECONDS,
                        new LinkedBlockingQueue<>(2 * nebulaConf.getMaxConnsSize()),
                        new ChunJunThreadFactory("nebulaAsyncExec"),
                        new ThreadPoolExecutor.CallerRunsPolicy());
    }

    @Override
    public void handleAsyncInvoke(CompletableFuture<Collection<RowData>> future, Object... keys)
            throws Exception {
        executor.execute(
                () -> {
                    queryData(future, keys);
                });
    }

    private void queryData(CompletableFuture<Collection<RowData>> future, Object... keys) {
        HashMap<String, Object> params = new HashMap<>();
        for (int i = 0; i < keyNames.length; i++) {
            params.put(keyNames[i], keys[i]);
        }
        String ngql =
                new LookupNGQLBuilder()
                        .setFieldNames(fieldNames)
                        .setNebulaConf(nebulaConf)
                        .setFiterFieldNames(keyNames)
                        .build();

        ResultSet resultSet = null;
        try {
            resultSet = nebulaSession.executeWithParameter(ngql, params);
        } catch (IOErrorException e) {
            LOG.error(
                    "execute ngql failed,massage: {},ngql: {},params: {}",
                    e.getMessage(),
                    ngql,
                    params,
                    e);
        }
        int size = resultSet.rowsSize();
        ArrayList<BaseTableRow> cacheContent = Lists.newArrayList();
        if (size > 0) {
            ArrayList<RowData> rowDatas = new ArrayList<>();
            for (int i = 0; i < size; i++) {
                ResultSet.Record valueWrappers = resultSet.rowValues(i);
                List<ValueWrapper> values = valueWrappers.values();
                BaseTableRow baseTableRow = new BaseTableRow(values);
                RowData rowData = null;
                try {
                    rowData = rowConverter.toInternal(baseTableRow);
                } catch (Exception e) {
                    LOG.error(
                            "convert data failed,massage: {},data : {}",
                            e.getMessage(),
                            baseTableRow);
                }
                if (openCache()) {
                    cacheContent.add(baseTableRow);
                }
                rowDatas.add(rowData);
            }
            dealCacheData(
                    buildCacheKey(keys),
                    CacheObj.buildCacheObj(ECacheContentType.MultiLine, cacheContent));
            future.complete(rowDatas);
        } else {
            dealMissKey(future);
            dealCacheData(buildCacheKey(keys), CacheMissVal.getMissKeyObj());
        }
    }

    @Override
    public void close() throws Exception {
        if (nebulaSession != null) {
            nebulaSession.close();
        }
    }
}
