/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.dtstack.flinkx.connector.sqlservercdc.entity;

import com.dtstack.flinkx.connector.sqlservercdc.util.SqlServerCdcUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Date: 2019/12/03 Company: www.dtstack.com
 *
 * <p>this class is copied from (https://github.com/debezium/debezium).
 *
 * @author tudou
 */
public class ChangeTablePointer {
    private static final Logger LOG = LoggerFactory.getLogger(ChangeTablePointer.class);

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

    /**
     * get data form resultSet
     *
     * @return
     * @throws SQLException
     */
    public Object[] getData() throws SQLException {
        final int dataColumnCount = resultSet.getMetaData().getColumnCount() - (COL_DATA - 1);
        final Object[] data = new Object[dataColumnCount];
        for (int i = 0; i < dataColumnCount; i++) {
            data[i] = resultSet.getObject(COL_DATA + i);
        }
        return data;
    }

    /**
     * get types from metadata
     *
     * @return
     * @throws SQLException
     */
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
            LOG.debug("Closing result set of change tables for table {}", changeTable);
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
        return "ChangeTablePointer [changeTable="
                + changeTable
                + ", resultSet="
                + resultSet
                + ", completed="
                + completed
                + ", currentChangePosition="
                + currentChangePosition
                + "]";
    }
}
