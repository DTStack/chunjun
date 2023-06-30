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
package com.dtstack.chunjun.connector.hive.source;

import com.dtstack.chunjun.config.TypeConfig;
import com.dtstack.chunjun.connector.hive.converter.HiveJdbcSyncConverter;
import com.dtstack.chunjun.connector.hive.converter.HiveRawTypeMapper;
import com.dtstack.chunjun.connector.hive.entity.ConnectionInfo;
import com.dtstack.chunjun.connector.hive.util.HiveDbUtil;
import com.dtstack.chunjun.connector.jdbc.source.JdbcInputFormat;
import com.dtstack.chunjun.connector.jdbc.source.JdbcInputSplit;
import com.dtstack.chunjun.util.ColumnBuildUtil;
import com.dtstack.chunjun.util.TableUtil;

import org.apache.flink.core.io.InputSplit;
import org.apache.flink.table.types.logical.RowType;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

@Slf4j
public class HiveInputFormat extends JdbcInputFormat {

    private ConnectionInfo connectionInfo;

    @Override
    public void openInternal(InputSplit inputSplit) {
        connectionInfo = new ConnectionInfo();
        connectionInfo.setJdbcUrl(jdbcConfig.getJdbcUrl());
        connectionInfo.setUsername(jdbcConfig.getUsername());
        connectionInfo.setPassword(jdbcConfig.getPassword());
        if (jdbcConfig.getQueryTimeOut() > 0) {
            connectionInfo.setTimeout(jdbcConfig.getQueryTimeOut());
        }
        this.currentJdbcInputSplit = (JdbcInputSplit) inputSplit;
        initMetric(currentJdbcInputSplit);
        if (!canReadData(currentJdbcInputSplit)) {
            log.warn(
                    "Not read data when the start location are equal to end location, start = {}, end = {}",
                    currentJdbcInputSplit.getStartLocation(),
                    currentJdbcInputSplit.getEndLocation());
            hasNext = false;
            return;
        }

        String querySQL = null;
        try {
            dbConn =
                    HiveDbUtil.getConnection(
                            connectionInfo,
                            getRuntimeContext().getDistributedCache(),
                            jobId,
                            String.valueOf(indexOfSubTask));
            dbConn.setAutoCommit(false);

            Pair<List<String>, List<TypeConfig>> pair = null;
            List<String> fullColumnList = new LinkedList<>();
            List<TypeConfig> fullColumnTypeList = new LinkedList<>();
            if (StringUtils.isBlank(jdbcConfig.getCustomSql())) {
                pair =
                        jdbcDialect.getTableMetaData(
                                dbConn,
                                jdbcConfig.getSchema(),
                                jdbcConfig.getTable(),
                                jdbcConfig.getQueryTimeOut(),
                                null,
                                null);
                fullColumnList = pair.getLeft();
                fullColumnTypeList = pair.getRight();
            }
            Pair<List<String>, List<TypeConfig>> columnPair =
                    ColumnBuildUtil.handleColumnList(
                            jdbcConfig.getColumn(), fullColumnList, fullColumnTypeList);
            columnNameList = columnPair.getLeft();
            columnTypeList = columnPair.getRight();

            querySQL = buildQuerySql(currentJdbcInputSplit);
            jdbcConfig.setQuerySql(querySQL);
            executeQuery(currentJdbcInputSplit.getStartLocation());
            // 增量任务
            needUpdateEndLocation =
                    jdbcConfig.isIncrement()
                            && !jdbcConfig.isPolling()
                            && !jdbcConfig.isUseMaxFunc();
            RowType rowType =
                    TableUtil.createRowType(
                            columnNameList, columnTypeList, HiveRawTypeMapper::apply);
            setRowConverter(
                    rowConverter == null
                            ? new HiveJdbcSyncConverter(rowType, jdbcConfig)
                            : rowConverter);
        } catch (SQLException se) {
            String expMsg = se.getMessage();
            expMsg = querySQL == null ? expMsg : expMsg + "\n querySQL: " + querySQL;
            throw new IllegalArgumentException("open() failed." + expMsg, se);
        }
    }

    /**
     * 执行查询
     *
     * @param startLocation
     * @throws SQLException
     */
    @Override
    public void executeQuery(String startLocation) throws SQLException {
        statement = dbConn.createStatement(resultSetType, resultSetConcurrency);
        statement.execute("SET hive.auto.convert.join=false");
        if (jdbcConfig.getProperties() != null) {
            String hiveSqlJobName = jdbcConfig.getProperties().getProperty("mapred.job.name", null);
            if (hiveSqlJobName != null) {
                statement.execute("set mapred.job.name=" + hiveSqlJobName);
            }
        }
        resultSet = statement.executeQuery(jdbcConfig.getQuerySql());
        hasNext = resultSet.next();
    }
}
