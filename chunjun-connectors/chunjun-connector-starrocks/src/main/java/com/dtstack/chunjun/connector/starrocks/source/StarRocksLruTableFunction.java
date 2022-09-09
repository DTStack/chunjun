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

import com.dtstack.chunjun.connector.starrocks.conf.StarRocksConf;
import com.dtstack.chunjun.connector.starrocks.source.be.StarRocksQueryPlanVisitor;
import com.dtstack.chunjun.connector.starrocks.source.be.StarRocksSourceBeReader;
import com.dtstack.chunjun.connector.starrocks.source.be.entity.QueryBeXTablets;
import com.dtstack.chunjun.connector.starrocks.source.be.entity.QueryInfo;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.enums.ECacheContentType;
import com.dtstack.chunjun.lookup.AbstractLruTableFunction;
import com.dtstack.chunjun.lookup.cache.CacheMissVal;
import com.dtstack.chunjun.lookup.cache.CacheObj;
import com.dtstack.chunjun.lookup.conf.LookupConf;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;
import com.dtstack.chunjun.throwable.NoRestartException;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

/** @author liuliu 2022/7/19 */
public class StarRocksLruTableFunction extends AbstractLruTableFunction {

    private static final Logger LOG = LoggerFactory.getLogger(StarRocksLruTableFunction.class);

    private final StarRocksConf starRocksConf;

    private final int[] keyIndexes;

    private StarRocksQueryPlanVisitor queryPlanVisitor;

    private final String queryStatement;

    public StarRocksLruTableFunction(
            StarRocksConf starRocksConf,
            LookupConf lookupConf,
            int[] keyIndexes,
            AbstractRowConverter rowConverter) {
        super(lookupConf, rowConverter);
        this.starRocksConf = starRocksConf;
        this.keyIndexes = keyIndexes;

        queryStatement = buildQueryStatement();
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        this.queryPlanVisitor = new StarRocksQueryPlanVisitor(starRocksConf);
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
                                                queryBeXTablets.getBeNode(), starRocksConf);
                                beReader.openScanner(
                                        queryBeXTablets.getTabletIds(),
                                        queryInfo.getQueryPlan().getOpaqued_query_plan());
                                beReader.startToRead();

                                List<RowData> rowDataList = new ArrayList<>();
                                List<Object[]> cacheContent = new ArrayList<>();
                                while (beReader.hasNext()) {
                                    Object[] next = beReader.getNext();
                                    cacheContent.add(next);
                                    try {
                                        GenericRowData rowData =
                                                (GenericRowData)
                                                        rowConverter.toInternalLookup(next);
                                        rowDataList.add(rowData);
                                    } catch (Exception e) {
                                        parseErrorRecords.inc();
                                        if (parseErrorRecords.getCount()
                                                > lookupConf.getErrorLimit()) {
                                            throw new NoRestartException(
                                                    "lru parse error time exceeded", e);
                                        }
                                    }
                                }
                                beReader.close();
                                if (rowDataList.size() > 0) {
                                    dealCacheData(
                                            cacheKey,
                                            CacheObj.buildCacheObj(
                                                    ECacheContentType.MultiLine, cacheContent));
                                    future.complete(rowDataList);
                                } else {
                                    dealMissKey(future);
                                    dealCacheData(cacheKey, CacheMissVal.getMissKeyObj());
                                }
                            });
        } catch (IOException e) {
            throw new ChunJunRuntimeException(e);
        }
    }

    public String buildQueryStatement() {
        return "select "
                + String.join(",", starRocksConf.getFieldNames())
                + " from "
                + starRocksConf.getDatabase()
                + "."
                + starRocksConf.getTable()
                + " where ";
    }

    public String buildFilterStatement(Object[] value) {
        StringBuilder builder = new StringBuilder(queryStatement);
        for (int i = 0; i < keyIndexes.length; i++) {
            String fieldName = starRocksConf.getFieldNames()[keyIndexes[i]];
            LogicalTypeRoot typeRoot =
                    starRocksConf.getDataTypes()[keyIndexes[i]].getLogicalType().getTypeRoot();
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
        LOG.info(String.format("startRocks lru querySql:%s", builder));
        return builder.toString();
    }

    @Override
    public void close() throws Exception {
        super.close();
    }
}
