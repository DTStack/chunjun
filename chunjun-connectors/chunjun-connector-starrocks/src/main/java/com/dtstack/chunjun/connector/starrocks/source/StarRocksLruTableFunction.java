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

package com.dtstack.chunjun.connector.starrocks.source;

import com.dtstack.chunjun.connector.starrocks.config.StarRocksConfig;
import com.dtstack.chunjun.connector.starrocks.source.be.StarRocksQueryPlanVisitor;
import com.dtstack.chunjun.connector.starrocks.source.be.StarRocksSourceBeReader;
import com.dtstack.chunjun.connector.starrocks.source.be.entity.QueryBeXTablets;
import com.dtstack.chunjun.connector.starrocks.source.be.entity.QueryInfo;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.enums.ECacheContentType;
import com.dtstack.chunjun.lookup.AbstractLruTableFunction;
import com.dtstack.chunjun.lookup.cache.CacheMissVal;
import com.dtstack.chunjun.lookup.cache.CacheObj;
import com.dtstack.chunjun.lookup.config.LookupConfig;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;
import com.dtstack.chunjun.throwable.NoRestartException;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.dtstack.chunjun.connector.starrocks.util.StarRocksUtil.splitQueryBeXTablets;

@Slf4j
public class StarRocksLruTableFunction extends AbstractLruTableFunction {

    private static final long serialVersionUID = -784897225682591517L;

    private final StarRocksConfig starRocksConfig;

    private final int[] keyIndexes;

    private StarRocksQueryPlanVisitor queryPlanVisitor;

    private String queryStatement;

    public StarRocksLruTableFunction(
            StarRocksConfig starRocksConfig,
            LookupConfig lookupConfig,
            int[] keyIndexes,
            AbstractRowConverter rowConverter) {
        super(lookupConfig, rowConverter);
        this.starRocksConfig = starRocksConfig;
        this.keyIndexes = keyIndexes;
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        this.queryPlanVisitor = new StarRocksQueryPlanVisitor(starRocksConfig);
        this.queryStatement = buildQueryStatement();
    }

    @Override
    public void handleAsyncInvoke(CompletableFuture<Collection<RowData>> future, Object... keys) {
        String cacheKey = buildCacheKey(keys);
        try {
            QueryInfo queryInfo = queryPlanVisitor.getQueryInfo(buildFilterStatement(keys));
            List<QueryBeXTablets> queryBeXTabletsList = splitQueryBeXTablets(1, queryInfo).get(0);
            queryBeXTabletsList
                    .parallelStream()
                    .forEach(
                            queryBeXTablets -> {
                                StarRocksSourceBeReader beReader =
                                        new StarRocksSourceBeReader(
                                                queryBeXTablets.getBeNode(), starRocksConfig);
                                beReader.openScanner(
                                        queryBeXTablets.getTabletIds(),
                                        queryInfo.getQueryPlan().getOpaqued_query_plan());
                                beReader.startToRead();
                                readAndDealData(beReader, cacheKey, future);
                            });
        } catch (IOException e) {
            throw new ChunJunRuntimeException(e);
        }
    }

    private void readAndDealData(
            StarRocksSourceBeReader beReader,
            String cacheKey,
            CompletableFuture<Collection<RowData>> future) {
        List<RowData> rowDataList = new ArrayList<>();
        List<Object[]> cacheContent = new ArrayList<>();
        try {
            while (beReader.hasNext()) {
                Object[] next = beReader.getNext();
                cacheContent.add(next);

                GenericRowData rowData = (GenericRowData) rowConverter.toInternalLookup(next);
                rowDataList.add(rowData);
            }
        } catch (Exception e) {
            parseErrorRecords.inc();
            if (parseErrorRecords.getCount() > lookupConfig.getErrorLimit()) {
                throw new NoRestartException("lru parse error time exceeded", e);
            }
        } finally {
            beReader.close();
        }
        dealResult(cacheKey, future, rowDataList, cacheContent);
    }

    private void dealResult(
            String cacheKey,
            CompletableFuture<Collection<RowData>> future,
            List<RowData> rowDataList,
            List<Object[]> cacheContent) {
        if (rowDataList.size() > 0) {
            dealCacheData(
                    cacheKey, CacheObj.buildCacheObj(ECacheContentType.MultiLine, cacheContent));
            future.complete(rowDataList);
        } else {
            dealMissKey(future);
            dealCacheData(cacheKey, CacheMissVal.getMissKeyObj());
        }
    }

    public String buildQueryStatement() {
        return "select "
                + String.join(",", starRocksConfig.getFieldNames())
                + " from "
                + starRocksConfig.getDatabase()
                + "."
                + starRocksConfig.getTable()
                + " where ";
    }

    public String buildFilterStatement(Object[] value) {
        StringBuilder builder = new StringBuilder(queryStatement);
        for (int i = 0; i < keyIndexes.length; i++) {
            String fieldName = starRocksConfig.getFieldNames()[keyIndexes[i]];
            LogicalTypeRoot typeRoot =
                    starRocksConfig.getDataTypes()[keyIndexes[i]].getLogicalType().getTypeRoot();
            Object curValue = value[i];
            if (curValue == null) {
                builder.append(fieldName).append("IS NULL");
            } else {
                if (typeRoot.name().startsWith("TIMESTAMP")) {
                    DateTimeFormatter dateTimeFormatter =
                            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");
                    curValue =
                            dateTimeFormatter.format(((TimestampData) curValue).toLocalDateTime());
                } else if (typeRoot == LogicalTypeRoot.DATE) {
                    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
                    Calendar c = Calendar.getInstance();
                    c.setTime(new Date(0L));
                    c.add(Calendar.DATE, (int) curValue);
                    curValue = dateFormat.format(c.getTime());
                }
                builder.append(fieldName).append("=").append(String.format("'%s'", curValue));
            }
            if (i + 1 != keyIndexes.length) {
                builder.append(" and ");
            }
        }
        log.info(String.format("startRocks lru querySql:%s", builder));
        return builder.toString();
    }
}
