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
package com.dtstack.chunjun.connector.jdbc.sink;

import com.dtstack.chunjun.cdc.DdlRowData;
import com.dtstack.chunjun.cdc.EventType;
import com.dtstack.chunjun.cdc.ddl.DdlRowDataConvented;
import com.dtstack.chunjun.cdc.ddl.definition.TableIdentifier;
import com.dtstack.chunjun.connector.jdbc.conf.JdbcConfig;
import com.dtstack.chunjun.connector.jdbc.dialect.JdbcDialect;
import com.dtstack.chunjun.connector.jdbc.statement.FieldNamedPreparedStatement;
import com.dtstack.chunjun.connector.jdbc.util.JdbcUtil;
import com.dtstack.chunjun.element.ColumnRowData;
import com.dtstack.chunjun.enums.EWriteMode;
import com.dtstack.chunjun.enums.Semantic;
import com.dtstack.chunjun.sink.format.BaseRichOutputFormat;
import com.dtstack.chunjun.throwable.WriteRecordException;
import com.dtstack.chunjun.util.ExceptionUtil;
import com.dtstack.chunjun.util.GsonUtil;
import com.dtstack.chunjun.util.JsonUtil;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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

    protected JdbcConfig jdbcConf;
    protected JdbcDialect jdbcDialect;

    protected transient Connection dbConn;

    protected transient PreparedStmtProxy stmtProxy;

    protected Set<TableIdentifier> createTableOnSnapShot = new HashSet<>();

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
            dbConn.setAutoCommit(jdbcConf.isAutoCommit());
            if (!EWriteMode.INSERT.name().equalsIgnoreCase(jdbcConf.getMode())) {
                List<String> updateKey = jdbcConf.getUniqueKey();
                if (CollectionUtils.isEmpty(updateKey)) {
                    List<String> tableIndex =
                            JdbcUtil.getTableUniqueIndex(
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
            stmtProxy =
                    new PreparedStmtProxy(
                            fieldNamedPreparedStatement,
                            rowConverter,
                            dbConn,
                            jdbcConf,
                            jdbcDialect);
        }
    }

    @Override
    protected void writeSingleRecordInternal(RowData row) throws WriteRecordException {
        int index = 0;
        try {
            stmtProxy.writeSingleRecordInternal(row);
            if (Semantic.EXACTLY_ONCE == semantic) {
                rowsOfCurrentTransaction += rows.size();
            } else {
                JdbcUtil.commit(dbConn);
            }
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
            }
            stmtProxy.executeBatch();
            // 开启了cp，但是并没有使用2pc方式让下游数据可见
            if (Semantic.EXACTLY_ONCE == semantic) {
                rowsOfCurrentTransaction += rows.size();
            } else {
                JdbcUtil.commit(dbConn);
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
        doCommit();
    }

    @Override
    public void rollback(long checkpointId) throws Exception {
        dbConn.rollback();
    }

    public void doCommit() throws SQLException {
        try {
            if (!jdbcConf.isAutoCommit()) {
                dbConn.commit();
            }
            snapshotWriteCounter.add(rowsOfCurrentTransaction);
            rowsOfCurrentTransaction = 0;
            stmtProxy.clearStatementCache();
        } catch (Exception e) {
            dbConn.rollback();
            throw e;
        }
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
                            LOG.info("add sql to batch, sql = {}", s);
                            stmt.addBatch(s);
                        }
                    }
                }
                stmt.executeBatch();
            } catch (SQLException e) {
                throw new RuntimeException(
                        "execute sql failed, sqlList = " + GsonUtil.GSON.toJson(sqlList), e);
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
    protected void preExecuteDdlRowData(DdlRowData rowData) throws Exception {
        while (this.rows.size() > 0) {
            super.writeRecordInternal();
        }
        doCommit();
    }

    @Override
    protected void executeDdlRowData(DdlRowData ddlRowData) throws Exception {
        if (ddlRowData.isSnapShot()) {
            TableIdentifier tableIdentifier = ddlRowData.getTableIdentifier();
            // 表已存在 且 createTableOnSnapShot 不包含 直接跳过改为已执行
            // 因为上游的一个create语句可能会被拆分为多条，所以不能仅仅判断数据库是否存在这个表
            if (!createTableOnSnapShot.contains(tableIdentifier)
                    && tableExist(
                            tableIdentifier.getDataBase(),
                            tableIdentifier.getSchema(),
                            tableIdentifier.getTable())) {
                executorService.execute(
                        () ->
                                ddlHandler.updateDDLChange(
                                        ddlRowData.getTableIdentifier(),
                                        ddlRowData.getLsn(),
                                        ddlRowData.getLsnSequence(),
                                        2,
                                        "table has exists so skip this snapshot data"));
                return;
            }
        }

        if (ddlRowData instanceof DdlRowDataConvented
                && !((DdlRowDataConvented) ddlRowData).conventSuccessful()) {
            return;
        }

        String sql = ddlRowData.getSql();
        String schema = ddlRowData.getTableIdentifier().getSchema();
        if (ddlRowData instanceof DdlRowDataConvented) {
            sql = ((DdlRowDataConvented) ddlRowData).getConventInfo();
            LOG.info(
                    "receive a convented ddlSql {} for table:{} and origin sql is {}",
                    ((DdlRowDataConvented) ddlRowData).getConventInfo(),
                    ddlRowData.getTableIdentifier().toString(),
                    ddlRowData.getSql());
        } else {
            LOG.info(
                    "receive a ddlSql {}  for table:{}",
                    ddlRowData.getSql(),
                    ddlRowData.getTableIdentifier().toString());
        }

        String finalSql = sql;
        executorService.execute(
                () -> {
                    try {
                        Statement statement = dbConn.createStatement();
                        if (StringUtils.isNotBlank(schema)
                                && !EventType.CREATE_SCHEMA.equals(ddlRowData.getType())) {
                            switchSchema(schema, statement);
                        }
                        statement.execute(finalSql);

                        if (ddlRowData.isSnapShot()) {
                            createTableOnSnapShot.add(ddlRowData.getTableIdentifier());
                        }

                        ddlHandler.updateDDLChange(
                                ddlRowData.getTableIdentifier(),
                                ddlRowData.getLsn(),
                                ddlRowData.getLsnSequence(),
                                2,
                                null);
                    } catch (Throwable e) {
                        LOG.warn("execute sql {} error", finalSql, e);
                        ddlHandler.updateDDLChange(
                                ddlRowData.getTableIdentifier(),
                                ddlRowData.getLsn(),
                                ddlRowData.getLsnSequence(),
                                -1,
                                ExceptionUtil.getErrorMessage(e));
                    }
                });
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

    protected void switchSchema(String schema, Statement statement) throws Exception {}

    public boolean tableExist(String catalogName, String schemaName, String tableName)
            throws SQLException {
        return dbConn.getMetaData()
                .getTables(catalogName, schemaName, tableName, new String[] {"TABLE"})
                .next();
    }

    public JdbcConfig getJdbcConf() {
        return jdbcConf;
    }

    public void setJdbcConf(JdbcConfig jdbcConf) {
        this.jdbcConf = jdbcConf;
    }

    public void setJdbcDialect(JdbcDialect jdbcDialect) {
        this.jdbcDialect = jdbcDialect;
    }

    public void setColumnNameList(List<String> columnNameList) {
        this.columnNameList = columnNameList;
    }

    public void setColumnTypeList(List<String> columnTypeList) {
        this.columnTypeList = columnTypeList;
    }
}
