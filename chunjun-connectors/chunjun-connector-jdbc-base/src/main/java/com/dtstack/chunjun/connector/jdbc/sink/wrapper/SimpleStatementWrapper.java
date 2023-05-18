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

public class SimpleStatementWrapper implements JdbcBatchStatementWrapper<RowData> {

    protected FieldNamedPreparedStatement statement;
    protected AbstractRowConverter rowConverter;

    public SimpleStatementWrapper(
            FieldNamedPreparedStatement statement, AbstractRowConverter converter) {
        this.statement = statement;
        rowConverter = converter;
    }

    @Override
    public void addToBatch(RowData record) throws Exception {
        rowConverter.toExternal(record, statement);
        statement.addBatch();
    }

    @Override
    public void executeBatch() throws SQLException {
        statement.executeBatch();
    }

    @Override
    public void writeSingleRecord(RowData record) throws Exception {
        rowConverter.toExternal(record, statement);
        statement.execute();
    }

    @Override
    public ResultSet executeQuery(RowData record) throws Exception {
        rowConverter.toInternal(record);
        return statement.executeQuery();
    }

    @Override
    public void clearParameters() throws SQLException {
        statement.clearParameters();
    }

    @Override
    public void close() throws SQLException {
        statement.close();
    }

    @Override
    public void clearBatch() throws SQLException {
        statement.clearBatch();
    }

    @Override
    public void reOpen(Connection connection) throws SQLException {
        statement.reOpen(connection);
    }

    @Override
    public void clearStatementCache() {}
}
