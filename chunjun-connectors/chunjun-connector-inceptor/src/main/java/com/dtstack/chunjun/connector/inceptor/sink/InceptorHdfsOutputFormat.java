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
package com.dtstack.chunjun.connector.inceptor.sink;

import com.dtstack.chunjun.connector.inceptor.conf.InceptorConf;
import com.dtstack.chunjun.connector.inceptor.dialect.InceptorHdfsDialect;
import com.dtstack.chunjun.connector.inceptor.util.InceptorDbUtil;
import com.dtstack.chunjun.connector.jdbc.conf.JdbcConf;
import com.dtstack.chunjun.connector.jdbc.sink.JdbcOutputFormat;
import com.dtstack.chunjun.connector.jdbc.util.JdbcUtil;
import com.dtstack.chunjun.element.ColumnRowData;
import com.dtstack.chunjun.enums.Semantic;
import com.dtstack.chunjun.throwable.WriteRecordException;
import com.dtstack.chunjun.util.GsonUtil;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;

/**
 * OutputFormat for writing data to relational database.
 *
 * <p>Company: www.dtstack.com
 *
 * @author dujie
 */
public class InceptorHdfsOutputFormat extends JdbcOutputFormat {

    protected static final Logger LOG = LoggerFactory.getLogger(InceptorHdfsOutputFormat.class);

    protected static final long serialVersionUID = 1L;

    protected InceptorConf inceptorConf;

    private SimpleDateFormat partitionFormat;

    // 当前分区
    private String currentPartition;

    // 是否是事务表
    private Boolean isTransactionTable;

    // 当前事务是否已开启
    private volatile boolean transactionStart;

    protected Statement statement;

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        super.open(taskNumber, numTasks);
    }

    @Override
    protected void openInternal(int taskNumber, int numTasks) {
        try {
            // when isTransactionTable is null, the tash is sql
            if (isTransactionTable == null) {
                partitionFormat = getPartitionFormat();
            }

            // batch and transactionTable
            if (isTransactionTable != null && isTransactionTable && this.batchSize == 1) {
                this.batchSize = 1024;
            }

            dbConn = getConnection();
            statement = dbConn.createStatement();
            // use database
            if (StringUtils.isNotBlank(jdbcConf.getSchema())) {
                statement.execute(
                        String.format("USE %s", jdbcDialect.quoteTable(jdbcConf.getSchema())));
            }
            initColumnList();
            switchNextPartiiton(new Date());

            // sql
            if (isTransactionTable == null) {
                isTransactionTable = isTransactionTable();
                if (isTransactionTable) {
                    super.checkpointMode = CheckpointingMode.EXACTLY_ONCE;
                    super.semantic = Semantic.EXACTLY_ONCE;
                }
            }
            LOG.info("subTask[{}}] wait finished", taskNumber);
        } catch (SQLException throwables) {
            throw new IllegalArgumentException("open() failed.", throwables);
        } finally {
            JdbcUtil.commit(dbConn);
        }
    }

    @Override
    protected void writeSingleRecordInternal(RowData rowData) throws WriteRecordException {
        int index = 0;
        try {
            switchNextPartiiton(new Date());
            if (!transactionStart) {
                statement.execute(InceptorDbUtil.INCEPTOR_TRANSACTION_TYPE);
                statement.execute(InceptorDbUtil.INCEPTOR_TRANSACTION_BEGIN);
                transactionStart = true;
            }
            stmtProxy.writeSingleRecordInternal(rowData);

            if (Semantic.EXACTLY_ONCE != semantic) {
                doCommit();
            }

        } catch (Exception e) {
            processWriteException(e, index, rowData);
        }
    }

    @Override
    public void preCommit() throws Exception {
        if (jdbcConf.getRestoreColumnIndex() > -1) {
            Object state;
            if (lastRow instanceof GenericRowData) {
                state = ((GenericRowData) lastRow).getField(jdbcConf.getRestoreColumnIndex());
            } else if (lastRow instanceof ColumnRowData) {
                state =
                        ((ColumnRowData) lastRow)
                                .getField(jdbcConf.getRestoreColumnIndex())
                                .asString();
            } else {
                LOG.warn("can't get [{}] from lastRow:{}", jdbcConf.getRestoreColumn(), lastRow);
                state = null;
            }
            formatState.setState(state);
        }

        if (rows != null && rows.size() > 0) {
            int size = rows.size();
            super.writeRecordInternal();
            numWriteCounter.add(size);
        } else {
            stmtProxy.executeBatch();
        }
    }

    @Override
    protected void writeMultipleRecordsInternal() throws Exception {
        try {
            switchNextPartiiton(new Date());
            if (!transactionStart) {
                Statement statement = dbConn.createStatement();
                statement.execute(InceptorDbUtil.INCEPTOR_TRANSACTION_TYPE);
                statement.execute(InceptorDbUtil.INCEPTOR_TRANSACTION_BEGIN);
                transactionStart = true;
            }
            for (RowData row : rows) {
                stmtProxy.convertToExternal(row);
                stmtProxy.addBatch();
                lastRow = row;
            }
            stmtProxy.executeBatch();

            // 开启了cp，但是并没有使用2pc方式让下游数据可见
            if (Semantic.EXACTLY_ONCE == semantic) {
                rowsOfCurrentTransaction += rows.size();
            } else {
                doCommit();
            }
        } catch (Exception e) {
            LOG.warn(
                    "write Multiple Records error, start to rollback connection, first row = {}",
                    rows.size() > 0 ? GsonUtil.GSON.toJson(rows.get(0)) : "null",
                    e);
            throw e;
        } finally {
            // 执行完后清空batch
            stmtProxy.clearBatch();
        }
    }

    public void doCommit() throws SQLException {
        try {
            if (transactionStart) {
                Statement statement = dbConn.createStatement();
                statement.execute(InceptorDbUtil.INCEPTOR_TRANSACTION_COMMIT);
            }

            snapshotWriteCounter.add(rowsOfCurrentTransaction);
            rowsOfCurrentTransaction = 0;
            stmtProxy.clearBatch();
        } catch (Exception e) {
            dbConn.rollback();
            throw e;
        } finally {
            transactionStart = false;
        }
    }

    @Override
    public void rollback(long checkpointId) throws Exception {
        try {
            if (transactionStart) {
                Statement statement = dbConn.createStatement();
                statement.execute(InceptorDbUtil.INCEPTOR_TRANSACTION_ROLLBACK);
            }
        } finally {
            transactionStart = false;
        }
    }

    @Override
    public Connection getConnection() {

        return InceptorDbUtil.getConnection(
                inceptorConf, getRuntimeContext().getDistributedCache(), jobId);
    }

    @Override
    protected String prepareTemplates() {
        String singleSql =
                ((InceptorHdfsDialect) jdbcDialect)
                        .getInsertPartitionIntoStatement(
                                inceptorConf.getSchema(),
                                inceptorConf.getTable(),
                                inceptorConf.getPartition(),
                                currentPartition,
                                columnNameList.toArray(new String[0]));

        LOG.info("write sql:{}", singleSql);
        return singleSql;
    }

    private SimpleDateFormat getPartitionFormat() {
        if (StringUtils.isBlank(inceptorConf.getPartitionType())) {
            throw new IllegalArgumentException("partitionEnumStr is empty!");
        }
        SimpleDateFormat format;
        switch (inceptorConf.getPartitionType().toUpperCase(Locale.ENGLISH)) {
            case "DAY":
                format = new SimpleDateFormat("yyyyMMdd");
                break;
            case "HOUR":
                format = new SimpleDateFormat("yyyyMMddHH");
                break;
            case "MINUTE":
                format = new SimpleDateFormat("yyyyMMddHHmm");
                break;
            default:
                throw new UnsupportedOperationException(
                        "partitionEnum = " + inceptorConf.getPartitionType() + " is undefined!");
        }
        TimeZone timeZone = TimeZone.getDefault();
        LOG.info("timeZone = {}", timeZone);
        format.setTimeZone(timeZone);
        return format;
    }

    private void switchNextPartiiton(Date currentData) throws SQLException {
        if (partitionFormat != null) {
            String newPartition = partitionFormat.format(currentData);
            if (StringUtils.isBlank(currentPartition) || !currentPartition.equals(newPartition)) {
                LOG.info(
                        "switch old partition {}  to new partition {}",
                        currentPartition,
                        newPartition);
                if (stmtProxy != null) {
                    stmtProxy.close();
                }
                currentPartition = newPartition;
            }
        }
        buildStmtProxy();
    }

    public void setJdbcConf(JdbcConf jdbcConf) {
        super.setJdbcConf(jdbcConf);
        this.inceptorConf = (InceptorConf) jdbcConf;
    }

    // 判断是否是事务表
    private boolean isTransactionTable() throws SQLException {
        Statement statement = dbConn.createStatement();
        ResultSet rs =
                statement.executeQuery(
                        "desc formatted " + jdbcDialect.quoteTable(jdbcConf.getTable()));
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();
        List<String> columnNames = new ArrayList<>(columnCount);
        for (int i = 0; i < columnCount; i++) {
            columnNames.add(metaData.getColumnName(i + 1));
        }

        List<Map<String, String>> data = new ArrayList<>();
        while (rs.next()) {
            Map<String, String> lineData =
                    new HashMap<>(Math.max((int) (columnCount / .75f) + 1, 16));
            for (String columnName : columnNames) {
                lineData.put(columnName, rs.getString(columnName));
            }
            data.add(lineData);
        }
        return data.stream()
                .filter(
                        i ->
                                i.values().stream()
                                        .anyMatch(
                                                i1 -> {
                                                    if (i1 != null) {
                                                        return i1.startsWith("transactional");
                                                    } else {
                                                        return false;
                                                    }
                                                }))
                .anyMatch(
                        i ->
                                i.values().stream()
                                        .anyMatch(
                                                i1 -> {
                                                    if (i1 != null) {
                                                        return i1.startsWith("true");
                                                    } else {
                                                        return false;
                                                    }
                                                }));
    }

    public void setTransactionTable(Boolean transactionTable) {
        isTransactionTable = transactionTable;
    }
}
