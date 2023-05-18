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
package com.dtstack.chunjun.connector.jdbc.sink.wrapper;

import com.dtstack.chunjun.connector.jdbc.statement.FieldNamedPreparedStatement;
import com.dtstack.chunjun.converter.AbstractRowConverter;

import org.apache.flink.table.data.RowData;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.function.Function;

public class InsertOrUpdateStatementWrapper implements JdbcBatchStatementWrapper<RowData> {

    private final FieldNamedPreparedStatement insertStatement;
    private final FieldNamedPreparedStatement updateStatement;
    private final FieldNamedPreparedStatement existStatement;

    private final Function<RowData, RowData> existFieldExtractor;

    private final AbstractRowConverter insertRowConverter;
    private final AbstractRowConverter updateRowConverter;
    private final AbstractRowConverter existRowConverter;

    public InsertOrUpdateStatementWrapper(
            FieldNamedPreparedStatement insertStatement,
            FieldNamedPreparedStatement updateStatement,
            FieldNamedPreparedStatement existStatement,
            Function<RowData, RowData> existFieldExtractor,
            AbstractRowConverter insertRowConverter,
            AbstractRowConverter updateRowConverter,
            AbstractRowConverter existRowConverter) {
        this.insertStatement = insertStatement;
        this.updateStatement = updateStatement;
        this.existStatement = existStatement;
        this.existFieldExtractor = existFieldExtractor;
        this.insertRowConverter = insertRowConverter;
        this.updateRowConverter = updateRowConverter;
        this.existRowConverter = existRowConverter;
    }

    @Override
    public void addToBatch(RowData record) throws Exception {
        if (isExist(record)) {
            updateRowConverter.toExternal(record, updateStatement);
            updateStatement.addBatch();
        } else {
            insertRowConverter.toExternal(record, insertStatement);
            insertStatement.addBatch();
        }
    }

    private boolean isExist(RowData record) throws Exception {
        existRowConverter.toExternal(existFieldExtractor.apply(record), existStatement);
        try (ResultSet resultSet = existStatement.executeQuery()) {
            return resultSet.next();
        }
    }

    @Override
    public void executeBatch() throws SQLException {
        updateStatement.executeBatch();
        insertStatement.executeBatch();
    }

    @Override
    public void writeSingleRecord(RowData record) throws Exception {
        if (isExist(record)) {
            updateRowConverter.toExternal(record, updateStatement);
            updateStatement.execute();
        } else {
            insertRowConverter.toExternal(record, insertStatement);
            insertStatement.execute();
        }
    }

    @Override
    public ResultSet executeQuery(RowData record) throws SQLException {
        throw new UnsupportedOperationException("executeQuery is not supported");
    }

    @Override
    public void clearParameters() throws SQLException {}

    @Override
    public void close() throws SQLException {
        for (FieldNamedPreparedStatement s :
                Arrays.asList(existStatement, insertStatement, updateStatement)) {
            if (s != null) {
                s.close();
            }
        }
    }

    @Override
    public void clearBatch() throws SQLException {
        for (FieldNamedPreparedStatement s :
                Arrays.asList(existStatement, insertStatement, updateStatement)) {
            if (s != null) {
                s.clearBatch();
            }
        }
    }

    @Override
    public void reOpen(Connection connection) throws SQLException {
        insertStatement.reOpen(connection);
        updateStatement.reOpen(connection);
        existStatement.reOpen(connection);
    }

    @Override
    public void clearStatementCache() {}
}
