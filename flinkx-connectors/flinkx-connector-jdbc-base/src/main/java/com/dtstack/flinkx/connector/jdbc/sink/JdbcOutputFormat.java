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
package com.dtstack.flinkx.connector.jdbc.sink;

import com.dtstack.flinkx.conf.FieldConf;
import com.dtstack.flinkx.connector.jdbc.conf.JdbcConf;
import com.dtstack.flinkx.connector.jdbc.dialect.JdbcDialect;
import com.dtstack.flinkx.connector.jdbc.statement.FieldNamedPreparedStatement;
import com.dtstack.flinkx.connector.jdbc.util.JdbcUtil;
import com.dtstack.flinkx.element.ColumnRowData;
import com.dtstack.flinkx.enums.EWriteMode;
import com.dtstack.flinkx.enums.Semantic;
import com.dtstack.flinkx.sink.format.BaseRichOutputFormat;
import com.dtstack.flinkx.throwable.WriteRecordException;
import com.dtstack.flinkx.util.ExceptionUtil;
import com.dtstack.flinkx.util.GsonUtil;
import com.dtstack.flinkx.util.JsonUtil;
import com.dtstack.flinkx.util.TableUtil;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * OutputFormat for writing data to relational database.
 *
 * <p>Company: www.dtstack.com
 *
 * @author huyifan.zju@163.com
 */
public class JdbcOutputFormat extends BaseRichOutputFormat {

    protected static final Logger LOG = LoggerFactory.getLogger(JdbcOutputFormat.class);

    protected static final long serialVersionUID = 1L;

    protected JdbcConf jdbcConf;
    protected JdbcDialect jdbcDialect;

    protected transient Connection dbConn;

    protected transient PreparedStmtProxy stmtProxy;

    @Override
    public void initializeGlobal(int parallelism) {
        executeBatch(jdbcConf.getPreSql());
    }

    @Override
    public void finalizeGlobal(int parallelism) {
        executeBatch(jdbcConf.getPostSql());
    }

    @Override
    protected void openInternal(int taskNumber, int numTasks) {
        try {
            dbConn = getConnection();
            // 默认关闭事务自动提交，手动控制事务
            if (Semantic.EXACTLY_ONCE == semantic) {
                dbConn.setAutoCommit(false);
            }
            initColumnList();
            if (!EWriteMode.INSERT.name().equalsIgnoreCase(jdbcConf.getMode())) {
                List<String> updateKey = jdbcConf.getUniqueKey();
                if (CollectionUtils.isEmpty(updateKey)) {
                    List<String> tableIndex =
                            JdbcUtil.getTableIndex(
                                    jdbcConf.getSchema(), jdbcConf.getTable(), dbConn);
                    jdbcConf.setUniqueKey(tableIndex);
                    LOG.info("updateKey = {}", JsonUtil.toJson(tableIndex));
                }
            }

            buildStmtProxy();
            LOG.info("subTask[{}}] wait finished", taskNumber);
        } catch (SQLException sqe) {
            throw new IllegalArgumentException("open() failed.", sqe);
        } finally {
            JdbcUtil.commit(dbConn);
        }
    }

    public void buildStmtProxy() throws SQLException {
        String tableInfo = jdbcConf.getTable();

        if ("*".equalsIgnoreCase(tableInfo)) {
            stmtProxy = new PreparedStmtProxy(dbConn, jdbcDialect, false);
        } else {
            FieldNamedPreparedStatement fieldNamedPreparedStatement =
                    FieldNamedPreparedStatement.prepareStatement(
                            dbConn, prepareTemplates(), this.columnNameList.toArray(new String[0]));
            RowType rowType =
                    TableUtil.createRowType(
                            columnNameList, columnTypeList, jdbcDialect.getRawTypeConverter());
            setRowConverter(
                    rowConverter == null
                            ? jdbcDialect.getColumnConverter(rowType, jdbcConf)
                            : rowConverter);
            stmtProxy =
                    new PreparedStmtProxy(
                            fieldNamedPreparedStatement,
                            rowConverter,
                            dbConn,
                            jdbcConf,
                            jdbcDialect);
        }
    }

    /** init columnNameList、 columnTypeList and hasConstantField */
    protected void initColumnList() {
        Pair<List<String>, List<String>> pair = getTableMetaData();

        List<FieldConf> fieldList = jdbcConf.getColumn();
        List<String> fullColumnList = pair.getLeft();
        List<String> fullColumnTypeList = pair.getRight();
        handleColumnList(fieldList, fullColumnList, fullColumnTypeList);
    }

    /**
     * for override. because some databases have case-sensitive metadata。
     *
     * @return
     */
    protected Pair<List<String>, List<String>> getTableMetaData() {
        return JdbcUtil.getTableMetaData(jdbcConf.getSchema(), jdbcConf.getTable(), dbConn);
    }

    /**
     * detailed logic for handling column
     *
     * @param fieldList
     * @param fullColumnList
     * @param fullColumnTypeList
     */
    protected void handleColumnList(
            List<FieldConf> fieldList,
            List<String> fullColumnList,
            List<String> fullColumnTypeList) {
        if (fieldList.size() == 1 && Objects.equals(fieldList.get(0).getName(), "*")) {
            columnNameList = fullColumnList;
            columnTypeList = fullColumnTypeList;
            return;
        }

        columnNameList = new ArrayList<>(fieldList.size());
        columnTypeList = new ArrayList<>(fieldList.size());
        for (FieldConf fieldConf : fieldList) {
            columnNameList.add(fieldConf.getName());
            for (int i = 0; i < fullColumnList.size(); i++) {
                if (fieldConf.getName().equalsIgnoreCase(fullColumnList.get(i))) {
                    columnTypeList.add(fullColumnTypeList.get(i));
                    break;
                }
            }
        }
    }

    @Override
    protected void writeSingleRecordInternal(RowData row) throws WriteRecordException {
        int index = 0;
        try {
            stmtProxy.writeSingleRecordInternal(row);
        } catch (Exception e) {
            JdbcUtil.rollBack(dbConn);
            processWriteException(e, index, row);
        }
    }

    @Override
    protected String recordConvertDetailErrorMessage(int pos, Object row) {
        return "\nJdbcOutputFormat ["
                + jobName
                + "] writeRecord error: when converting field["
                + pos
                + "] in Row("
                + row
                + ")";
    }

    @Override
    protected void writeMultipleRecordsInternal() throws Exception {
        try {
            for (RowData row : rows) {
                stmtProxy.convertToExternal(row);
                stmtProxy.addBatch();
                lastRow = row;
            }
            stmtProxy.executeBatch();
            // 开启了cp，但是并没有使用2pc方式让下游数据可见
            if (Semantic.EXACTLY_ONCE == semantic) {
                rowsOfCurrentTransaction += rows.size();
            }
        } catch (Exception e) {
            LOG.warn(
                    "write Multiple Records error, start to rollback connection, row size = {}, first row = {}",
                    rows.size(),
                    rows.size() > 0 ? GsonUtil.GSON.toJson(rows.get(0)) : "null",
                    e);
            JdbcUtil.rollBack(dbConn);
            throw e;
        } finally {
            // 执行完后清空batch
            stmtProxy.clearBatch();
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
            super.writeRecordInternal();
        } else {
            stmtProxy.executeBatch();
        }
    }

    @Override
    public void commit(long checkpointId) throws Exception {
        try {
            dbConn.commit();
            snapshotWriteCounter.add(rowsOfCurrentTransaction);
            rowsOfCurrentTransaction = 0;
            stmtProxy.clearBatch();
        } catch (Exception e) {
            dbConn.rollback();
            throw e;
        }
    }

    @Override
    public void rollback(long checkpointId) throws Exception {
        dbConn.rollback();
    }

    /**
     * 执行pre、post SQL
     *
     * @param sqlList
     */
    protected void executeBatch(List<String> sqlList) {
        if (CollectionUtils.isNotEmpty(sqlList)) {
            try (Connection conn = getConnection();
                    Statement stmt = conn.createStatement()) {
                for (String sql : sqlList) {
                    // 兼容多条SQL写在同一行的情况
                    String[] strings = sql.split(";");
                    for (String s : strings) {
                        if (StringUtils.isNotBlank(s)) {
                            LOG.info("add sql to batch, sql = {}", sql);
                            stmt.addBatch(sql);
                        }
                    }
                }
                stmt.executeBatch();
            } catch (SQLException e) {
                LOG.error("execute sql failed, sqlList = {}, e = {}", sqlList, e);
            }
        }
    }

    protected String prepareTemplates() {
        String singleSql;
        if (EWriteMode.INSERT.name().equalsIgnoreCase(jdbcConf.getMode())) {
            singleSql =
                    jdbcDialect.getInsertIntoStatement(
                            jdbcConf.getSchema(),
                            jdbcConf.getTable(),
                            columnNameList.toArray(new String[0]));
        } else if (EWriteMode.REPLACE.name().equalsIgnoreCase(jdbcConf.getMode())) {
            singleSql =
                    jdbcDialect
                            .getReplaceStatement(
                                    jdbcConf.getSchema(),
                                    jdbcConf.getTable(),
                                    columnNameList.toArray(new String[0]))
                            .get();
        } else if (EWriteMode.UPDATE.name().equalsIgnoreCase(jdbcConf.getMode())) {
            singleSql =
                    jdbcDialect
                            .getUpsertStatement(
                                    jdbcConf.getSchema(),
                                    jdbcConf.getTable(),
                                    columnNameList.toArray(new String[0]),
                                    jdbcConf.getUniqueKey().toArray(new String[0]),
                                    jdbcConf.isAllReplace())
                            .get();
        } else {
            throw new IllegalArgumentException("Unknown write mode:" + jdbcConf.getMode());
        }

        LOG.info("write sql:{}", singleSql);
        return singleSql;
    }

    protected void processWriteException(Exception e, int index, RowData row)
            throws WriteRecordException {
        if (e instanceof SQLException) {
            if (e.getMessage().contains("No operations allowed")) {
                throw new RuntimeException("Connection maybe closed", e);
            }
        }

        if (index < row.getArity()) {
            String message = recordConvertDetailErrorMessage(index, row);
            throw new WriteRecordException(message, e, index, row);
        }
        throw new WriteRecordException(e.getMessage(), e);
    }

    @Override
    public void closeInternal() {
        snapshotWriteCounter.add(rowsOfCurrentTransaction);
        try {
            if (stmtProxy != null) {
                stmtProxy.close();
            }
        } catch (SQLException e) {
            LOG.error(ExceptionUtil.getErrorMessage(e));
        }
        JdbcUtil.closeDbResources(null, null, dbConn, true);
    }

    /**
     * 获取数据库连接，用于子类覆盖
     *
     * @return connection
     */
    protected Connection getConnection() throws SQLException {
        return JdbcUtil.getConnection(jdbcConf, jdbcDialect);
    }

    public JdbcConf getJdbcConf() {
        return jdbcConf;
    }

    public void setJdbcConf(JdbcConf jdbcConf) {
        this.jdbcConf = jdbcConf;
    }

    public void setJdbcDialect(JdbcDialect jdbcDialect) {
        this.jdbcDialect = jdbcDialect;
    }
}
