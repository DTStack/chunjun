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

package com.dtstack.chunjun.connector.kudu.table.lookup;

import com.dtstack.chunjun.connector.kudu.config.KuduCommonConfig;
import com.dtstack.chunjun.connector.kudu.config.KuduLookupConfig;
import com.dtstack.chunjun.connector.kudu.util.KuduUtil;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.enums.ECacheContentType;
import com.dtstack.chunjun.lookup.AbstractLruTableFunction;
import com.dtstack.chunjun.lookup.cache.CacheMissVal;
import com.dtstack.chunjun.lookup.cache.CacheObj;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.client.AsyncKuduClient;
import org.apache.kudu.client.AsyncKuduScanner;
import org.apache.kudu.client.KuduPredicate;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.RowResult;
import org.apache.kudu.client.RowResultIterator;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

@Slf4j
public class KuduLruTableFunction extends AbstractLruTableFunction {

    private static final long serialVersionUID = -4266958307581889540L;

    private AsyncKuduClient client;

    private KuduTable kuduTable;

    private AsyncKuduScanner.AsyncKuduScannerBuilder scannerBuilder;

    private final KuduLookupConfig kuduLookupConfig;

    private final String[] fieldNames;

    private final String[] keyNames;

    /** 缓存条数 */
    private static final Long FETCH_SIZE = 1000L;

    public KuduLruTableFunction(
            KuduLookupConfig lookupConfig,
            AbstractRowConverter<?, ?, ?, ?> rowConverter,
            String[] fieldNames,
            String[] keyNames) {
        super(lookupConfig, rowConverter);
        this.kuduLookupConfig = lookupConfig;
        this.fieldNames = fieldNames;
        this.keyNames = keyNames;
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        getKudu();
    }

    @Override
    public void handleAsyncInvoke(CompletableFuture<Collection<RowData>> future, Object... keys)
            throws Exception {
        String key = buildCacheKey(keys);

        if (StringUtils.isBlank(key)) {
            return;
        }

        Schema schema = kuduTable.getSchema();
        // scannerBuilder 设置为null重新加载过滤条件,然后connKudu重新赋值
        scannerBuilder = buildAsyncKuduScannerBuilder(schema, keys);

        List<Map<String, Object>> cacheContent = Lists.newArrayList();
        AsyncKuduScanner asyncKuduScanner = scannerBuilder.build();
        List<RowData> rowList = Lists.newArrayList();

        // 谓词

        Deferred<RowResultIterator> iteratorDeferred = asyncKuduScanner.nextRows();

        iteratorDeferred.addCallback(
                new GetListRowCB(
                        cacheContent, rowList, asyncKuduScanner, future, buildCacheKey(keys)));
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (Objects.nonNull(client)) {
            try {
                client.close();
            } catch (Exception e) {
                log.error("Error while closing client.", e);
            }
        }

        kuduTable = null;
        scannerBuilder = null;
    }

    private void getKudu() throws IOException {
        checkKuduTable();
        scannerBuilder = client.newScannerBuilder(kuduTable);
        Integer batchSizeBytes = kuduLookupConfig.getBatchSizeBytes();
        Long limitNum = kuduLookupConfig.getLimitNum();
        Boolean isFaultTolerant = kuduLookupConfig.getIsFaultTolerant();

        if (null == limitNum || limitNum <= 0) {
            scannerBuilder.limit(FETCH_SIZE);
        } else {
            scannerBuilder.limit(limitNum);
        }
        if (Objects.nonNull(batchSizeBytes)) {
            scannerBuilder.batchSizeBytes(batchSizeBytes);
        }
        if (Objects.nonNull(isFaultTolerant)) {
            scannerBuilder.setFaultTolerant(isFaultTolerant);
        }

        List<String> projectColumns = Arrays.asList(keyNames);
        scannerBuilder.setProjectedColumnNames(projectColumns);
    }

    private AsyncKuduScanner.AsyncKuduScannerBuilder buildAsyncKuduScannerBuilder(
            Schema schema, Object... keys) throws IOException {
        List<Object> keysList = Arrays.asList(keys);
        checkKuduTable();
        AsyncKuduScanner.AsyncKuduScannerBuilder scannerBuilder =
                client.newScannerBuilder(kuduTable);
        for (Object key : keys) {
            scannerBuilder.addPredicate(
                    KuduPredicate.newInListPredicate(
                            schema.getColumn((keyNames[keysList.indexOf(key)])),
                            Collections.singletonList(key)));
        }
        Integer batchSizeBytes = kuduLookupConfig.getBatchSizeBytes();
        Long limitNum = kuduLookupConfig.getLimitNum();
        Boolean isFaultTolerant = kuduLookupConfig.getIsFaultTolerant();

        if (Objects.isNull(limitNum) || limitNum <= 0) {
            scannerBuilder.limit(FETCH_SIZE);
        } else {
            scannerBuilder.limit(limitNum);
        }
        if (Objects.nonNull(batchSizeBytes)) {
            scannerBuilder.batchSizeBytes(batchSizeBytes);
        }
        if (Objects.nonNull(isFaultTolerant)) {
            scannerBuilder.setFaultTolerant(isFaultTolerant);
        }

        List<String> projectColumns = Arrays.asList(fieldNames);
        scannerBuilder.setProjectedColumnNames(projectColumns);
        return scannerBuilder;
    }

    private void checkKuduTable() throws IOException {
        try {
            if (Objects.isNull(kuduTable)) {
                KuduCommonConfig commonConfig = kuduLookupConfig.getCommonConfig();
                String tableName = kuduLookupConfig.getTableName();
                client = KuduUtil.getAsyncKuduClient(commonConfig);
                if (!client.syncClient().tableExists(tableName)) {
                    throw new IllegalArgumentException(
                            "Table Open Failed , please check table exists");
                }
                kuduTable = client.syncClient().openTable(tableName);
                log.info("connect kudu is succeed!");
            }
        } catch (Exception e) {
            throw new IOException("kudu table error!", e);
        }
    }

    class GetListRowCB implements Callback<Deferred<List<RowData>>, RowResultIterator> {
        private final List<Map<String, Object>> cacheContent;
        private final List<RowData> rowDataList;
        private final AsyncKuduScanner asyncKuduScanner;
        private final CompletableFuture<Collection<RowData>> future;
        private final String key;

        GetListRowCB(
                List<Map<String, Object>> cacheContent,
                List<RowData> rowDataList,
                AsyncKuduScanner asyncKuduScanner,
                CompletableFuture<Collection<RowData>> future,
                String key) {
            this.cacheContent = cacheContent;
            this.rowDataList = rowDataList;
            this.asyncKuduScanner = asyncKuduScanner;
            this.future = future;
            this.key = key;
        }

        @Override
        @SuppressWarnings("unchecked")
        public Deferred<List<RowData>> call(RowResultIterator results) throws Exception {
            for (RowResult result : results) {
                Map<String, Object> oneRow = Maps.newHashMap();

                List<ColumnSchema> columnSchemaList = result.getSchema().getColumns();
                for (ColumnSchema columnSchema : columnSchemaList) {
                    if (Objects.nonNull(columnSchema)) {
                        KuduUtil.setMapValue(
                                columnSchema.getType(), oneRow, columnSchema.getName(), result);
                    }
                }

                RowData rowData = rowConverter.toInternalLookup(result);
                if (openCache()) {
                    cacheContent.add(oneRow);
                }
                rowDataList.add(rowData);
            }
            if (asyncKuduScanner.hasMoreRows()) {
                return asyncKuduScanner.nextRows().addCallbackDeferring(this);
            }

            if (rowDataList.size() > 0) {
                if (openCache()) {
                    putCache(
                            key, CacheObj.buildCacheObj(ECacheContentType.MultiLine, cacheContent));
                }
                future.complete(rowDataList);
            } else {
                dealMissKey(future);
                if (openCache()) {
                    // 放置在putCache的Miss中 一段时间内同一个key都会直接返回
                    putCache(key, CacheMissVal.getMissKeyObj());
                }
            }

            return null;
        }
    }
}
