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

package com.dtstack.chunjun.connector.clickhouse.source;

import com.dtstack.chunjun.connector.clickhouse.util.ClickhouseUtil;
import com.dtstack.chunjun.connector.jdbc.source.JdbcInputFormat;
import com.dtstack.chunjun.connector.jdbc.source.JdbcInputSplit;

import org.apache.flink.core.io.InputSplit;

import java.sql.Connection;
import java.sql.SQLException;

public class ClickhouseInputFormat extends JdbcInputFormat {

    @Override
    public void openInternal(InputSplit inputSplit) {
        JdbcInputSplit jdbcInputSplit = (JdbcInputSplit) inputSplit;
        initMetric(jdbcInputSplit);
        if (!canReadData(jdbcInputSplit)) {
            LOG.warn(
                    "Not read data when the start location are equal to end location, start = {}, end = {}",
                    jdbcInputSplit.getStartLocation(),
                    jdbcInputSplit.getEndLocation());
            hasNext = false;
            return;
        }

        String querySQL = null;
        try {
            dbConn = getConnection();
            querySQL = buildQuerySql(jdbcInputSplit);
            jdbcConf.setQuerySql(querySQL);
            executeQuery(jdbcInputSplit.getStartLocation());
            // 增量任务
            needUpdateEndLocation =
                    jdbcConf.isIncrement() && !jdbcConf.isPolling() && !jdbcConf.isUseMaxFunc();
        } catch (SQLException se) {
            String expMsg = se.getMessage();
            expMsg = querySQL == null ? expMsg : expMsg + "\n querySQL: " + querySQL;
            throw new IllegalArgumentException("open() failed." + expMsg, se);
        }
    }

    @Override
    protected Connection getConnection() throws SQLException {
        return ClickhouseUtil.getConnection(
                jdbcConf.getJdbcUrl(), jdbcConf.getUsername(), jdbcConf.getPassword());
    }
}
