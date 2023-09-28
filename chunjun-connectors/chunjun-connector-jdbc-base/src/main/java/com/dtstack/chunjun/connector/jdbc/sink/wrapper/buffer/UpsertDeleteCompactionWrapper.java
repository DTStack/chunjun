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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.data.RowData;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static com.dtstack.chunjun.connector.jdbc.sink.wrapper.StatementWrapperUtil.changeFlag;

public class UpsertDeleteCompactionWrapper implements JdbcBatchStatementWrapper<RowData> {

    private final JdbcBatchStatementWrapper<RowData> upsertExecutor;
    private final JdbcBatchStatementWrapper<RowData> deleteExecutor;
    private final Function<RowData, RowData> keyExtractor;

    Map<RowData, Tuple2<Boolean, RowData>> buffer = new HashMap<>();

    public UpsertDeleteCompactionWrapper(
            JdbcBatchStatementWrapper<RowData> upsertExecutor,
            JdbcBatchStatementWrapper<RowData> deleteExecutor,
            Function<RowData, RowData> keyExtractor) {
        this.upsertExecutor = upsertExecutor;
        this.deleteExecutor = deleteExecutor;
        this.keyExtractor = keyExtractor;
    }

    @Override
    public void addToBatch(RowData record) {
        RowData key = keyExtractor.apply(record);
        boolean flag = changeFlag(record.getRowKind());
        buffer.put(key, Tuple2.of(flag, record));
    }

    @Override
    public void executeBatch() throws Exception {
        if (!buffer.isEmpty()) {
            for (Map.Entry<RowData, Tuple2<Boolean, RowData>> entry : buffer.entrySet()) {
                if (entry.getValue().f0) {
                    upsertExecutor.addToBatch(entry.getValue().f1);
                } else {
                    // delete by key
                    deleteExecutor.addToBatch(entry.getKey());
                }
            }
            deleteExecutor.executeBatch();
            upsertExecutor.executeBatch();
            buffer.clear();
        }
    }

    @Override
    public void writeSingleRecord(RowData record) throws Exception {
        boolean flag = changeFlag(record.getRowKind());
        if (flag) {
            upsertExecutor.writeSingleRecord(record);
        } else {
            deleteExecutor.writeSingleRecord(keyExtractor.apply(record));
        }
    }

    @Override
    public ResultSet executeQuery(RowData record) throws SQLException {
        throw new UnsupportedOperationException("executeQuery is not supported");
    }

    @Override
    public void clearParameters() throws SQLException {
        upsertExecutor.clearParameters();
        deleteExecutor.clearParameters();
    }

    @Override
    public void close() throws SQLException {
        upsertExecutor.close();
        deleteExecutor.close();
    }

    @Override
    public void clearBatch() throws SQLException {
        deleteExecutor.clearBatch();
        upsertExecutor.clearBatch();
    }

    @Override
    public void reOpen(Connection connection) throws SQLException {
        deleteExecutor.reOpen(connection);
        upsertExecutor.reOpen(connection);
    }

    @Override
    public void clearStatementCache() {}
}
