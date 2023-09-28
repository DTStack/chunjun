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
package com.dtstack.chunjun.connector.jdbc.sink.wrapper.buffer;

import com.dtstack.chunjun.connector.jdbc.sink.wrapper.JdbcBatchStatementWrapper;

import org.apache.flink.table.data.RowData;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public class InsertOrUpdateCompactionWrapper implements JdbcBatchStatementWrapper<RowData> {

    JdbcBatchStatementWrapper<RowData> insertExecutor;
    JdbcBatchStatementWrapper<RowData> isExistExecutor;
    JdbcBatchStatementWrapper<RowData> updateExecutor;
    Function<RowData, RowData> keyExtractor;
    Map<RowData, RowData> buffer = new HashMap<>();

    @Override
    public void addToBatch(RowData record) throws Exception {
        RowData key = keyExtractor.apply(record);
        buffer.put(key, record);
    }

    @Override
    public void executeBatch() throws Exception {
        if (!buffer.isEmpty()) {
            for (Map.Entry<RowData, RowData> entry : buffer.entrySet()) {
                RowData key = entry.getKey();
                RowData record = entry.getValue();
                if (isExist(key)) {
                    updateExecutor.addToBatch(record);
                } else {
                    insertExecutor.addToBatch(record);
                }
            }
            insertExecutor.executeBatch();
            updateExecutor.executeBatch();
        }
    }

    private boolean isExist(RowData key) throws Exception {
        ResultSet resultSet = isExistExecutor.executeQuery(key);
        return resultSet.next();
    }

    @Override
    public void writeSingleRecord(RowData record) throws Exception {
        if (isExist(record)) {
            updateExecutor.writeSingleRecord(record);
        } else {
            insertExecutor.writeSingleRecord(record);
        }
    }

    @Override
    public ResultSet executeQuery(RowData record) throws SQLException {
        throw new UnsupportedOperationException("executeQuery is not supported");
    }

    @Override
    public void clearParameters() throws SQLException {
        insertExecutor.clearParameters();
        isExistExecutor.clearParameters();
        updateExecutor.clearParameters();
    }

    @Override
    public void close() throws SQLException {
        insertExecutor.close();
        isExistExecutor.close();
        updateExecutor.close();
    }

    @Override
    public void clearBatch() throws SQLException {
        insertExecutor.clearBatch();
        isExistExecutor.clearBatch();
        updateExecutor.clearBatch();
    }

    @Override
    public void reOpen(Connection connection) throws SQLException {
        insertExecutor.reOpen(connection);
        isExistExecutor.reOpen(connection);
        updateExecutor.reOpen(connection);
    }

    @Override
    public void clearStatementCache() {}
}
