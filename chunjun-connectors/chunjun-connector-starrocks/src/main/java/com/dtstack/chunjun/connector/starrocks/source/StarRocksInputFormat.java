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
import com.dtstack.chunjun.constants.ConstantValue;
import com.dtstack.chunjun.source.format.BaseRichInputFormat;
import com.dtstack.chunjun.throwable.ReadRecordException;

import org.apache.flink.core.io.InputSplit;
import org.apache.flink.table.data.RowData;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.dtstack.chunjun.connector.starrocks.util.StarRocksUtil.splitQueryBeXTablets;

@Slf4j
public class StarRocksInputFormat extends BaseRichInputFormat {

    private static final long serialVersionUID = 8315233040239002065L;

    private StarRocksConfig starRocksConfig;

    private StarRocksSourceBeReader reader;

    @Override
    protected InputSplit[] createInputSplitsInternal(int minNumSplits) throws Exception {
        String querySql = getQueryStatement();
        log.info(String.format("starRocksInputFormat querySql is %s", querySql));
        StarRocksQueryPlanVisitor queryPlanVisitor = new StarRocksQueryPlanVisitor(starRocksConfig);
        QueryInfo queryInfo = queryPlanVisitor.getQueryInfo(querySql);
        List<List<QueryBeXTablets>> lists = splitQueryBeXTablets(minNumSplits, queryInfo);
        List<QueryBeXTablets> queryBeXTabletsList = new ArrayList<>();
        lists.forEach(queryBeXTabletsList::addAll);

        AtomicInteger index = new AtomicInteger();
        List<StarRocksInputSplit> res = new ArrayList<>();
        queryBeXTabletsList.forEach(
                queryBeXTablets ->
                        res.add(
                                new StarRocksInputSplit(
                                        index.getAndIncrement(),
                                        queryBeXTabletsList.size(),
                                        queryBeXTablets,
                                        queryInfo.getQueryPlan().getOpaqued_query_plan())));
        return res.toArray(new StarRocksInputSplit[0]);
    }

    @Override
    protected void openInternal(InputSplit inputSplit) {
        StarRocksInputSplit starRocksInputSplit = ((StarRocksInputSplit) inputSplit);
        QueryBeXTablets queryBeXTablets = starRocksInputSplit.getQueryBeXTablets();
        reader = new StarRocksSourceBeReader(queryBeXTablets.getBeNode(), starRocksConfig);
        reader.openScanner(
                queryBeXTablets.getTabletIds(), starRocksInputSplit.getOpaquedQueryPlan());
        reader.startToRead();
    }

    @Override
    protected RowData nextRecordInternal(RowData rowData) throws ReadRecordException {
        try {
            return rowConverter.toInternal(reader.getNext());
        } catch (Exception e) {
            throw new ReadRecordException("", e);
        }
    }

    @Override
    protected void closeInternal() {
        if (reader != null) {
            reader.close();
        }
    }

    @Override
    public boolean reachedEnd() {
        return !reader.hasNext();
    }

    public StarRocksConfig getStarRocksConf() {
        return starRocksConfig;
    }

    public void setStarRocksConf(StarRocksConfig starRocksConfig) {
        this.starRocksConfig = starRocksConfig;
    }

    public String getQueryStatement() {
        StringBuilder builder = new StringBuilder("select ");
        if (starRocksConfig.getColumn().size() == 1
                && starRocksConfig.getColumn().get(0).getName().equals(ConstantValue.STAR_SYMBOL)) {
            builder.append(ConstantValue.STAR_SYMBOL);
        } else {
            builder.append(String.join(",", starRocksConfig.getFieldNames()));
        }
        builder.append(" from ")
                .append(
                        String.format(
                                "%s.%s",
                                starRocksConfig.getDatabase(), starRocksConfig.getTable()));
        if (StringUtils.isNotBlank(starRocksConfig.getFilterStatement())) {
            builder.append(" where ").append(starRocksConfig.getFilterStatement());
        }
        return builder.toString();
    }
}
