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

package com.dtstack.flinkx.connector.clickhouse.sink;

import com.dtstack.flinkx.connector.clickhouse.util.CkProperties;
import com.dtstack.flinkx.connector.clickhouse.util.ClickhouseUtil;
import com.dtstack.flinkx.connector.clickhouse.util.pool.CkClientFactory;
import com.dtstack.flinkx.connector.clickhouse.util.pool.CkClientPool;
import com.dtstack.flinkx.connector.jdbc.sink.JdbcOutputFormat;
import com.dtstack.flinkx.connector.jdbc.statement.FieldNamedPreparedStatement;
import com.dtstack.flinkx.throwable.WriteRecordException;
import com.dtstack.flinkx.util.GsonUtil;

import org.apache.flink.table.data.RowData;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * @program: flinkx
 * @author: xiuzhu
 * @create: 2021/05/10
 */
public class ClickhouseOutputFormat extends JdbcOutputFormat {

    protected CkClientPool ckClientPool;

    @Override
    protected void openInternal(int taskNumber, int numTasks) {
        super.openInternal(taskNumber, numTasks);
        CkClientFactory clientFactory = new CkClientFactory();
        CkProperties ckProperties = new CkProperties();
        ckProperties.setUrl(jdbcConf.getJdbcUrl());
        ckProperties.setPassword(jdbcConf.getPassword());
        ckProperties.setUsername(jdbcConf.getUsername());
        clientFactory.setCkProperties(ckProperties);
        ckClientPool = new CkClientPool(clientFactory);
        LOG.info("init ck client pool");
    }

    @Override
    protected void writeSingleRecordInternal(RowData row) throws WriteRecordException {
        Connection newConn = ckClientPool.borrowObject();
        try {
            FieldNamedPreparedStatement fieldNamedPreparedStatement =
                    FieldNamedPreparedStatement.prepareStatement(
                            newConn,
                            prepareTemplates(),
                            this.columnNameList.toArray(new String[0]));
            fieldNamedPreparedStatement =
                    (FieldNamedPreparedStatement)
                            rowConverter.toExternal(row, fieldNamedPreparedStatement);
            fieldNamedPreparedStatement.execute();
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        } finally {
            ckClientPool.returnObject(newConn);
        }
    }

    @Override
    protected void writeMultipleRecordsInternal() throws Exception {
        Connection newConn = ckClientPool.borrowObject();
        try {
            FieldNamedPreparedStatement fieldNamedPreparedStatement =
                    FieldNamedPreparedStatement.prepareStatement(
                            newConn,
                            prepareTemplates(),
                            this.columnNameList.toArray(new String[0]));

            for (RowData row : rows) {
                fieldNamedPreparedStatement =
                        (FieldNamedPreparedStatement)
                                rowConverter.toExternal(row, fieldNamedPreparedStatement);
                fieldNamedPreparedStatement.addBatch();
                lastRow = row;
            }
            fieldNamedPreparedStatement.executeBatch();
        } catch (Exception e) {
            LOG.warn(
                    "write Multiple Records error, start to rollback connection, row size = {}, first row = {}",
                    rows.size(),
                    rows.size() > 0 ? GsonUtil.GSON.toJson(rows.get(0)) : "null",
                    e);
            throw e;
        } finally {
            ckClientPool.returnObject(newConn);
        }
    }

    @Override
    protected Connection getConnection() throws SQLException {
        return ClickhouseUtil.getConnection(
                jdbcConf.getJdbcUrl(), jdbcConf.getUsername(), jdbcConf.getPassword());
    }
}
