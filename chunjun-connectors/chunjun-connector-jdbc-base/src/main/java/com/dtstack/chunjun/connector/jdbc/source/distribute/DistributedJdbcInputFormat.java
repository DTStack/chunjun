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
package com.dtstack.chunjun.connector.jdbc.source.distribute;

import com.dtstack.chunjun.connector.jdbc.config.DataSourceConfig;
import com.dtstack.chunjun.connector.jdbc.source.JdbcInputFormat;
import com.dtstack.chunjun.connector.jdbc.util.JdbcUtil;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;
import com.dtstack.chunjun.util.GsonUtil;
import com.dtstack.chunjun.util.RangeSplitUtil;

import org.apache.flink.core.io.InputSplit;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

@Slf4j
public class DistributedJdbcInputFormat extends JdbcInputFormat {

    private static final long serialVersionUID = 3985627300325628890L;

    protected List<DataSourceConfig> sourceList;
    protected DistributedJdbcInputSplit inputSplit;
    protected int sourceIndex = 0;
    protected boolean noDataSource = false;

    @Override
    public void openInternal(InputSplit inputSplit) {
        this.inputSplit = (DistributedJdbcInputSplit) inputSplit;
        this.sourceList = this.inputSplit.getSourceList();
        if (CollectionUtils.isEmpty(this.sourceList)) {
            noDataSource = true;
            return;
        }
        log.info("DistributedJdbcInputFormat[{}]open: end", inputSplit);
    }

    @Override
    public InputSplit[] createInputSplitsInternal(int minNumSplits) {
        if (minNumSplits != jdbcConfig.getParallelism()) {
            throw new ChunJunRuntimeException(
                    String.format(
                            "numTaskVertices is [%s], but parallelism in jdbcConfig is [%s]",
                            minNumSplits, jdbcConfig.getParallelism()));
        }
        DistributedJdbcInputSplit[] inputSplits = new DistributedJdbcInputSplit[minNumSplits];
        List<List<DataSourceConfig>> subList =
                RangeSplitUtil.subListBySegment(sourceList, minNumSplits);

        for (int i = 0; i < subList.size(); i++) {
            DistributedJdbcInputSplit split =
                    new DistributedJdbcInputSplit(
                            i,
                            minNumSplits,
                            subList.get(i),
                            jdbcConfig.getSplitStrategy(),
                            jdbcConfig.isPolling());
            inputSplits[i] = split;
        }

        log.info(
                "create InputSplits successfully, inputSplits = {}",
                GsonUtil.GSON.toJson(inputSplits));
        return inputSplits;
    }

    @Override
    public boolean reachedEnd() {
        if (noDataSource) {
            return true;
        }
        try {
            if (dbConn == null) {
                openNextSource();
            }

            if (!hasNext) {
                if (sourceIndex + 1 < sourceList.size()) {
                    closeInternal();
                    sourceIndex++;
                    return reachedEnd();
                }
            }

            return !hasNext;
        } catch (SQLException e) {
            closeInternal();
            log.error("", e);
            throw new ChunJunRuntimeException(e);
        }
    }

    protected void openNextSource() throws SQLException {
        DataSourceConfig currentSource = sourceList.get(sourceIndex);
        dbConn = getConnection();
        dbConn.setAutoCommit(false);
        statement = dbConn.createStatement(resultSetType, resultSetConcurrency);

        statement.setFetchSize(jdbcConfig.getFetchSize());
        statement.setQueryTimeout(jdbcConfig.getQueryTimeOut());

        String querySql = buildQuerySql(inputSplit);
        jdbcConfig.setQuerySql(querySql);
        resultSet = statement.executeQuery(querySql);
        hasNext = resultSet.next();

        log.info(
                "open source: {}, table: {}", currentSource.getJdbcUrl(), currentSource.getTable());
    }

    @Override
    public void closeInternal() {
        JdbcUtil.closeDbResources(resultSet, statement, dbConn, true);
        dbConn = null;
        statement = null;
        resultSet = null;
    }

    @Override
    protected Connection getConnection() throws SQLException {
        DataSourceConfig currentSource = sourceList.get(sourceIndex);
        jdbcConfig.setJdbcUrl(currentSource.getJdbcUrl());
        jdbcConfig.setUsername(currentSource.getUserName());
        jdbcConfig.setPassword(currentSource.getPassword());
        jdbcConfig.setTable(currentSource.getTable());
        jdbcConfig.setSchema(currentSource.getSchema());
        return JdbcUtil.getConnection(jdbcConfig, this.jdbcDialect);
    }

    public void setSourceList(List<DataSourceConfig> sourceList) {
        this.sourceList = sourceList;
    }
}
