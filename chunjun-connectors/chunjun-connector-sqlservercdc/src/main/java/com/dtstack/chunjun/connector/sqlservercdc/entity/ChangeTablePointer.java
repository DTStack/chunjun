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

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.dtstack.chunjun.connector.sqlservercdc.entity;

import com.dtstack.chunjun.connector.sqlservercdc.util.SqlServerCdcUtil;

import lombok.extern.slf4j.Slf4j;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

/**
 * this class is copied from (<a href="https://github.com/debezium/debezium">class from
 * debezium</a>).
 */
@Slf4j
public class ChangeTablePointer {

    private static final int COL_COMMIT_LSN = 1;
    private static final int COL_ROW_LSN = 2;
    private static final int COL_OPERATION = 3;
    private static final int COL_DATA = 5;

    private final ChangeTable changeTable;
    private final Statement statement;
    private final ResultSet resultSet;
    private boolean completed = false;
    private TxLogPosition currentChangePosition;

    public ChangeTablePointer(
            ChangeTable changeTable, SqlServerCdcUtil.StatementResult statementResult) {
        this.changeTable = changeTable;
        this.resultSet = statementResult.getResultSet();
        this.statement = statementResult.getStatement();
    }

    public ChangeTable getChangeTable() {
        return changeTable;
    }

    public TxLogPosition getChangePosition() {
        return currentChangePosition;
    }

    public int getOperation() throws SQLException {
        return resultSet.getInt(COL_OPERATION);
    }

    public Object[] getData() throws SQLException {
        final int dataColumnCount = resultSet.getMetaData().getColumnCount() - (COL_DATA - 1);
        final Object[] data = new Object[dataColumnCount];
        for (int i = 0; i < dataColumnCount; i++) {
            data[i] = resultSet.getObject(COL_DATA + i);
        }
        return data;
    }

    public List<String> getTypes() throws SQLException {
        final int dataColumnCount = resultSet.getMetaData().getColumnCount() - (COL_DATA - 1);
        List<String> columnTypes = new ArrayList<>();
        for (int i = 0; i < dataColumnCount; i++) {
            columnTypes.add(resultSet.getMetaData().getColumnTypeName(COL_DATA + i));
        }
        return columnTypes;
    }

    public boolean next() throws SQLException {
        completed = !resultSet.next();
        currentChangePosition =
                completed
                        ? TxLogPosition.NULL
                        : TxLogPosition.valueOf(
                                Lsn.valueOf(resultSet.getBytes(COL_COMMIT_LSN)),
                                Lsn.valueOf(resultSet.getBytes(COL_ROW_LSN)));
        if (completed) {
            log.debug("Closing result set of change tables for table {}", changeTable);
            resultSet.close();
            statement.close();
        }
        return !completed;
    }

    public boolean isCompleted() {
        return completed;
    }

    public int compareTo(ChangeTablePointer o) {
        return getChangePosition().compareTo(o.getChangePosition());
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", ChangeTablePointer.class.getSimpleName() + "[", "]")
                .add("changeTable=" + changeTable)
                .add("statement=" + statement)
                .add("resultSet=" + resultSet)
                .add("completed=" + completed)
                .add("currentChangePosition=" + currentChangePosition)
                .toString();
    }
}
