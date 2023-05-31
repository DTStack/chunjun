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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static com.dtstack.chunjun.connector.jdbc.sink.wrapper.StatementWrapperUtil.changeFlag;

public class InsertDeleteCompactionWrapper implements JdbcBatchStatementWrapper<RowData> {

    protected final JdbcBatchStatementWrapper<RowData> insertExecutor;
    protected final JdbcBatchStatementWrapper<RowData> deleteExecutor;
    private final Function<RowData, RowData> keyExtractor;

    Map<RowData, RowData> insertBuffer = new HashMap<>();
    Set<RowData> deleteBuffer = new HashSet<>();

    public InsertDeleteCompactionWrapper(
            JdbcBatchStatementWrapper<RowData> insertExecutor,
            JdbcBatchStatementWrapper<RowData> deleteExecutor,
            Function<RowData, RowData> keyExtractor) {
        this.insertExecutor = insertExecutor;
        this.deleteExecutor = deleteExecutor;
        this.keyExtractor = keyExtractor;
    }

    @Override
    public void addToBatch(RowData record) {
        RowData key = keyExtractor.apply(record);
        boolean flag = changeFlag(record.getRowKind());
        if (flag) {
            insertBuffer.put(key, record);
        } else {
            if (insertBuffer.containsKey(key)) {
                insertBuffer.remove(key);
            } else {
                deleteBuffer.add(key);
            }
        }
    }

    @Override
    public void executeBatch() throws Exception {
        if (!deleteBuffer.isEmpty()) {
            for (RowData key : deleteBuffer) {
                deleteExecutor.addToBatch(key);
            }
            deleteExecutor.executeBatch();
            deleteBuffer.clear();
        }
        if (!insertBuffer.isEmpty()) {
            for (RowData record : insertBuffer.values()) {
                insertExecutor.addToBatch(record);
            }
            insertExecutor.executeBatch();
            insertBuffer.clear();
        }
    }

    @Override
    public void writeSingleRecord(RowData record) throws Exception {
        boolean flag = changeFlag(record.getRowKind());
        if (flag) {
            insertExecutor.writeSingleRecord(record);
        } else {
            RowData key = keyExtractor.apply(record);
            deleteExecutor.writeSingleRecord(key);
        }
    }

    @Override
    public ResultSet executeQuery(RowData record) throws Exception {
        throw new UnsupportedOperationException("executeQuery is not supported");
    }

    @Override
    public void clearParameters() throws SQLException {
        deleteExecutor.clearParameters();
        insertExecutor.clearParameters();
    }

    @Override
    public void close() throws SQLException {
        deleteExecutor.close();
        insertExecutor.close();
    }

    @Override
    public void clearBatch() throws SQLException {
        deleteExecutor.clearBatch();
        insertExecutor.clearBatch();
    }

    @Override
    public void reOpen(Connection connection) throws SQLException {
        deleteExecutor.reOpen(connection);
        insertExecutor.reOpen(connection);
    }

    @Override
    public void clearStatementCache() {
        deleteExecutor.clearStatementCache();
        insertExecutor.clearStatementCache();
    }
}
