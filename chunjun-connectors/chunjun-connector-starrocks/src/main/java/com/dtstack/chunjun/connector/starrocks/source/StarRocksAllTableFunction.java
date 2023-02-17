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
import com.dtstack.chunjun.lookup.AbstractAllTableFunction;
import com.dtstack.chunjun.lookup.config.LookupConfig;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.functions.FunctionContext;

import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.dtstack.chunjun.connector.starrocks.util.StarRocksUtil.splitQueryBeXTablets;

@Slf4j
public class StarRocksAllTableFunction extends AbstractAllTableFunction {

    private static final long serialVersionUID = -4561229223877402123L;

    private final StarRocksConfig starRocksConfig;

    private final int[] keyIndexes;

    private final String querySql;

    private StarRocksQueryPlanVisitor queryPlanVisitor;

    public StarRocksAllTableFunction(
            StarRocksConfig starRocksConfig,
            LookupConfig lookupConfig,
            int[] keyIndexes,
            AbstractRowConverter rowConverter) {
        super(starRocksConfig.getFieldNames(), null, lookupConfig, rowConverter);
        this.keyIndexes = keyIndexes;
        this.starRocksConfig = starRocksConfig;
        this.querySql = buildQueryStatement();
    }

    @Override
    protected void initCache() {
        Map<String, List<GenericRowData>> newCache = Maps.newConcurrentMap();
        cacheRef.set(newCache);
        loadData(newCache);
    }

    @Override
    protected void reloadCache() {
        // reload cacheRef and replace to old cacheRef
        Map<String, List<GenericRowData>> newCache = Maps.newConcurrentMap();
        try {
            loadData(newCache);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        cacheRef.set(newCache);
        log.info(
                "----- " + lookupConfig.getTableName() + ": all cacheRef reload end:{}",
                LocalDateTime.now());
    }

    @Override
    protected void loadData(Object cache) {
        try {
            QueryInfo queryInfo = queryPlanVisitor.getQueryInfo(querySql);
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
                                while (beReader.hasNext()) {
                                    Object[] next = beReader.getNext();
                                    try {
                                        GenericRowData rowData =
                                                (GenericRowData)
                                                        rowConverter.toInternalLookup(next);
                                        // add cache
                                        ((Map<String, List<GenericRowData>>) cache)
                                                .computeIfAbsent(
                                                        buildCacheKey(rowData),
                                                        key -> new ArrayList<>())
                                                .add(rowData);
                                    } catch (Exception e) {
                                        throw new ChunJunRuntimeException("", e);
                                    }
                                }
                                beReader.close();
                            });
        } catch (IOException e) {
            throw new ChunJunRuntimeException("query from be failed", e);
        }
    }

    public String buildCacheKey(GenericRowData rowData) {
        return Arrays.stream(keyIndexes)
                .mapToObj(index -> String.valueOf(rowData.getField(index)))
                .collect(Collectors.joining("_"));
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        queryPlanVisitor = new StarRocksQueryPlanVisitor(starRocksConfig);
        super.open(context);
    }

    private String buildQueryStatement() {
        String QUERY_SQL_TEMPLATE = "select %s from `%s`.`%s`";
        return String.format(
                QUERY_SQL_TEMPLATE,
                String.join(",", fieldsName),
                starRocksConfig.getDatabase(),
                starRocksConfig.getTable());
    }

    @Override
    public void close() throws Exception {
        super.close();
    }
}
