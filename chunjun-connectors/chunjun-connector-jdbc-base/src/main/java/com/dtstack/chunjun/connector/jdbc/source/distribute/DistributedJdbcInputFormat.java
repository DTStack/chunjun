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

import com.dtstack.chunjun.conf.FieldConf;
import com.dtstack.chunjun.connector.jdbc.conf.DataSourceConf;
import com.dtstack.chunjun.connector.jdbc.source.JdbcInputFormat;
import com.dtstack.chunjun.connector.jdbc.util.JdbcUtil;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;
import com.dtstack.chunjun.util.ColumnBuildUtil;
import com.dtstack.chunjun.util.GsonUtil;
import com.dtstack.chunjun.util.RangeSplitUtil;
import com.dtstack.chunjun.util.TableUtil;

import org.apache.flink.core.io.InputSplit;
import org.apache.flink.table.types.logical.RowType;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/**
 * Date: 2022/01/12 Company: www.dtstack.com
 *
 * @author tudou
 */
public class DistributedJdbcInputFormat extends JdbcInputFormat {

    protected List<DataSourceConf> sourceList;
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
        for (FieldConf fieldConf : jdbcConf.getColumn()) {
            this.columnNameList.add(fieldConf.getName());
            this.columnTypeList.add(fieldConf.getType());
        }
        Pair<List<String>, List<String>> columnPair =
                ColumnBuildUtil.handleColumnList(
                        jdbcConf.getColumn(), this.columnNameList, this.columnTypeList);
        this.columnNameList = columnPair.getLeft();
        this.columnTypeList = columnPair.getRight();
        RowType rowType =
                TableUtil.createRowType(
                        columnNameList, columnTypeList, jdbcDialect.getRawTypeConverter());
        setRowConverter(
                this.rowConverter == null
                        ? jdbcDialect.getColumnConverter(rowType, jdbcConf)
                        : rowConverter);
        LOG.info("DistributedJdbcInputFormat[{}]open: end", inputSplit);
    }

    @Override
    public InputSplit[] createInputSplitsInternal(int minNumSplits) {
        if (minNumSplits != jdbcConf.getParallelism()) {
            throw new ChunJunRuntimeException(
                    String.format(
                            "numTaskVertices is [%s], but parallelism in jdbcConf is [%s]",
                            minNumSplits, jdbcConf.getParallelism()));
        }
        DistributedJdbcInputSplit[] inputSplits = new DistributedJdbcInputSplit[minNumSplits];
        List<List<DataSourceConf>> subList =
                RangeSplitUtil.subListBySegment(sourceList, minNumSplits);

        for (int i = 0; i < subList.size(); i++) {
            DistributedJdbcInputSplit split =
                    new DistributedJdbcInputSplit(i, minNumSplits, subList.get(i));
            inputSplits[i] = split;
        }

        LOG.info(
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
            LOG.error("", e);
            throw new ChunJunRuntimeException(e);
        }
    }

    protected void openNextSource() throws SQLException {
        DataSourceConf currentSource = sourceList.get(sourceIndex);
        dbConn = getConnection();
        dbConn.setAutoCommit(false);
        statement = dbConn.createStatement(resultSetType, resultSetConcurrency);

        statement.setFetchSize(jdbcConf.getFetchSize());
        statement.setQueryTimeout(jdbcConf.getQueryTimeOut());

        String querySql = buildQuerySql(inputSplit);
        jdbcConf.setQuerySql(querySql);
        resultSet = statement.executeQuery(querySql);
        columnCount = resultSet.getMetaData().getColumnCount();
        hasNext = resultSet.next();

        LOG.info(
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
        DataSourceConf currentSource = sourceList.get(sourceIndex);
        jdbcConf.setJdbcUrl(currentSource.getJdbcUrl());
        jdbcConf.setUsername(currentSource.getUserName());
        jdbcConf.setPassword(currentSource.getPassword());
        jdbcConf.setTable(currentSource.getTable());
        jdbcConf.setSchema(currentSource.getSchema());
        return JdbcUtil.getConnection(jdbcConf, this.jdbcDialect);
    }

    public void setSourceList(List<DataSourceConf> sourceList) {
        this.sourceList = sourceList;
    }
}
