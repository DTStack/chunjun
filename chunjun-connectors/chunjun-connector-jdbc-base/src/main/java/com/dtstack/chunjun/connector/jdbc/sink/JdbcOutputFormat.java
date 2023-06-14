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
import com.dtstack.chunjun.config.TypeConfig;
import com.dtstack.chunjun.connector.jdbc.config.JdbcConfig;
import com.dtstack.chunjun.connector.jdbc.dialect.JdbcDialect;
import com.dtstack.chunjun.connector.jdbc.sink.wrapper.InsertOrUpdateStatementWrapper;
import com.dtstack.chunjun.connector.jdbc.sink.wrapper.JdbcBatchStatementWrapper;
import com.dtstack.chunjun.connector.jdbc.sink.wrapper.SimpleStatementWrapper;
import com.dtstack.chunjun.connector.jdbc.sink.wrapper.buffer.UpsertDeleteCompactionWrapper;
import com.dtstack.chunjun.connector.jdbc.sink.wrapper.proxy.RestoreWrapperProxy;
import com.dtstack.chunjun.connector.jdbc.statement.FieldNamedPreparedStatement;
import com.dtstack.chunjun.connector.jdbc.util.JdbcUtil;
import com.dtstack.chunjun.converter.AbstractRowConverter;
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
import org.apache.flink.table.types.logical.RowType;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/** OutputFormat for writing data to relational database. */
@Slf4j
public class JdbcOutputFormat extends BaseRichOutputFormat {

    protected static final long serialVersionUID = 1L;

    protected JdbcConfig jdbcConfig;
    protected JdbcDialect jdbcDialect;

    protected transient Connection dbConn;

    protected transient JdbcBatchStatementWrapper<RowData> statementWrapper;

    protected Set<TableIdentifier> createTableOnSnapShot = new HashSet<>();

    private AbstractRowConverter keyRowConverter;
    private RowType keyRowType;

    @Override
    public void initializeGlobal(int parallelism) {
        executeBatch(jdbcConfig.getPreSql());
    }

    @Override
    public void finalizeGlobal(int parallelism) {
        executeBatch(jdbcConfig.getPostSql());
    }

    @Override
    protected void openInternal(int taskNumber, int numTasks) {
        try {
            dbConn = getConnection();
            // 默认关闭事务自动提交，手动控制事务
            dbConn.setAutoCommit(jdbcConfig.isAutoCommit());
            if (!EWriteMode.INSERT.name().equalsIgnoreCase(jdbcConfig.getMode())) {
                List<String> updateKey = jdbcConfig.getUniqueKey();
                if (CollectionUtils.isEmpty(updateKey)) {
                    List<String> tableIndex =
                            JdbcUtil.getTableUniqueIndex(
                                    jdbcDialect.getTableIdentify(
                                            jdbcConfig.getSchema(), jdbcConfig.getTable()),
                                    dbConn);
                    jdbcConfig.setUniqueKey(tableIndex);
                    log.info("updateKey = {}", JsonUtil.toJson(tableIndex));
                }
            }

            buildStatementWrapper();
            log.info("subTask[{}}] wait finished", taskNumber);
        } catch (SQLException sqe) {
            throw new IllegalArgumentException("open() failed.", sqe);
        } finally {
            JdbcUtil.commit(dbConn);
        }
    }

    public void buildStatementWrapper() throws SQLException {
        String tableInfo = jdbcConfig.getTable();

        if ("*".equalsIgnoreCase(tableInfo)) {
            // restoration
            statementWrapper = new RestoreWrapperProxy(dbConn, jdbcDialect, false);
        } else {
            if (useAbstractColumn || CollectionUtils.isEmpty(jdbcConfig.getUniqueKey())) {
                // sync or sql appendOnly
                FieldNamedPreparedStatement fieldNamedPreparedStatement =
                        FieldNamedPreparedStatement.prepareStatement(
                                dbConn,
                                prepareTemplates(),
                                this.columnNameList.toArray(new String[0]));
                statementWrapper =
                        new SimpleStatementWrapper(fieldNamedPreparedStatement, rowConverter);
            } else {
                // sql retract
                buildRetractStatementExecutor();
            }
        }
    }

    private void buildRetractStatementExecutor() throws SQLException {
        SimpleStatementWrapper deleteExecutor =
                new SimpleStatementWrapper(
                        FieldNamedPreparedStatement.prepareStatement(
                                dbConn,
                                jdbcDialect.getKeyedDeleteStatement(
                                        jdbcConfig.getSchema(),
                                        jdbcConfig.getTable(),
                                        jdbcConfig.getUniqueKey()),
                                jdbcConfig.getUniqueKey().toArray(new String[0])),
                        keyRowConverter);
        JdbcBatchStatementWrapper<RowData> upsertExecutor;
        if (jdbcDialect.supportUpsert()) {
            upsertExecutor =
                    new SimpleStatementWrapper(
                            FieldNamedPreparedStatement.prepareStatement(
                                    dbConn,
                                    getUpsertStatement(),
                                    this.columnNameList.toArray(new String[0])),
                            rowConverter);
        } else {
            upsertExecutor = getInsertOrUpdateExecutor();
        }
        statementWrapper =
                new UpsertDeleteCompactionWrapper(
                        upsertExecutor,
                        deleteExecutor,
                        JdbcUtil.getKeyExtractor(
                                columnNameList, jdbcConfig.getUniqueKey(), keyRowType, false));
    }

    private JdbcBatchStatementWrapper<RowData> getInsertOrUpdateExecutor() throws SQLException {
        FieldNamedPreparedStatement insertStatement =
                FieldNamedPreparedStatement.prepareStatement(
                        dbConn,
                        getInsertPrepareTemplate(),
                        this.columnNameList.toArray(new String[0]));
        FieldNamedPreparedStatement updateStatement =
                FieldNamedPreparedStatement.prepareStatement(
                        dbConn,
                        getUpdatePrepareTemplate(),
                        this.columnNameList.toArray(new String[0]));
        FieldNamedPreparedStatement selectStatement =
                FieldNamedPreparedStatement.prepareStatement(
                        dbConn,
                        jdbcDialect.getSelectFromStatement(
                                jdbcConfig.getSchema(),
                                jdbcConfig.getTable(),
                                jdbcConfig.getUniqueKey().toArray(new String[0])),
                        jdbcConfig.getUniqueKey().toArray(new String[0]));
        return new InsertOrUpdateStatementWrapper(
                insertStatement,
                updateStatement,
                selectStatement,
                JdbcUtil.getKeyExtractor(
                        columnNameList, jdbcConfig.getUniqueKey(), keyRowType, false),
                rowConverter,
                rowConverter,
                keyRowConverter);
    }

    @Override
    protected void writeSingleRecordInternal(RowData row) throws WriteRecordException {
        int index = 0;
        try {
            // 解决写入时数据库连接无效问题
            try {
                if (!dbConn.isValid(10)) {
                    dbConn = getConnection();
                    buildStatementWrapper();
                    log.warn("write single record reconnecting");
                }
            } catch (Throwable e) {
                // 解决 sqlserver JtdsConnection 没有实现 isValid 方法
                if (!(e instanceof AbstractMethodError)) {
                    throw new WriteRecordException("write single record error", e, 0, row);
                }
            }
            statementWrapper.writeSingleRecord(row);
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
            // 解决写入时数据库连接无效问题
            try {
                if (!dbConn.isValid(10)) {
                    dbConn = getConnection();
                    buildStatementWrapper();
                    log.warn("write multiple records reconnecting");
                }
            } catch (Throwable e) {
                // 解决 sqlserver JtdsConnection 没有实现 isValid 方法
                if (!(e instanceof AbstractMethodError)) {
                    throw e;
                }
            }
            for (RowData row : rows) {
                statementWrapper.addToBatch(row);
                lastRow = row;
            }
            statementWrapper.executeBatch();
            // 开启了cp，但是并没有使用2pc方式让下游数据可见
            if (Semantic.EXACTLY_ONCE == semantic) {
                rowsOfCurrentTransaction += rows.size();
            } else {
                JdbcUtil.commit(dbConn);
            }
        } catch (Exception e) {
            log.warn(
                    "write Multiple Records error, start to rollback connection, row size = {}, first row = {}",
                    rows.size(),
                    rows.size() > 0 ? GsonUtil.GSON.toJson(rows.get(0)) : "null",
                    e);
            JdbcUtil.rollBack(dbConn);
            throw e;
        } finally {
            // 执行完后清空batch
            statementWrapper.clearBatch();
        }
    }

    @Override
    public void preCommit() throws Exception {
        if (jdbcConfig.getRestoreColumnIndex() > -1) {
            Object state;
            if (lastRow instanceof GenericRowData) {
                state = ((GenericRowData) lastRow).getField(jdbcConfig.getRestoreColumnIndex());
            } else if (lastRow instanceof ColumnRowData) {
                state =
                        ((ColumnRowData) lastRow)
                                .getField(jdbcConfig.getRestoreColumnIndex())
                                .asString();
            } else {
                log.warn("can't get [{}] from lastRow:{}", jdbcConfig.getRestoreColumn(), lastRow);
                state = null;
            }
            formatState.setState(state);
        }

        if (rows != null && rows.size() > 0) {
            super.writeRecordInternal();
        } else {
            statementWrapper.executeBatch();
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
            if (!jdbcConfig.isAutoCommit()) {
                dbConn.commit();
            }
            snapshotWriteCounter.add(rowsOfCurrentTransaction);
            rowsOfCurrentTransaction = 0;
            statementWrapper.clearStatementCache();
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
                            log.info("add sql to batch, sql = {}", s);
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

    protected String getInsertPrepareTemplate() {
        return jdbcDialect.getInsertIntoStatement(
                jdbcConfig.getSchema(),
                jdbcConfig.getTable(),
                columnNameList.toArray(new String[0]));
    }

    protected String getUpdatePrepareTemplate() {
        return jdbcDialect.getUpdateStatement(
                jdbcConfig.getSchema(),
                jdbcConfig.getTable(),
                columnNameList.toArray(new String[0]),
                jdbcConfig.getUniqueKey().toArray(new String[0]));
    }

    protected String getReplacePrepareTemplate() {
        Optional<String> replaceStatement =
                jdbcDialect.getReplaceStatement(
                        jdbcConfig.getSchema(),
                        jdbcConfig.getTable(),
                        columnNameList.toArray(new String[0]));
        if (replaceStatement.isPresent()) {
            return replaceStatement.get();
        } else {
            throw new IllegalArgumentException("Unknown write mode:" + jdbcConfig.getMode());
        }
    }

    protected String getUpsertStatement() {
        Optional<String> upsertStatement =
                jdbcDialect.getUpsertStatement(
                        jdbcConfig.getSchema(),
                        jdbcConfig.getTable(),
                        columnNameList.toArray(new String[0]),
                        jdbcConfig.getUniqueKey().toArray(new String[0]),
                        jdbcConfig.isAllReplace());
        if (upsertStatement.isPresent()) {
            return upsertStatement.get();
        } else {
            throw new IllegalArgumentException("Unknown write mode:" + jdbcConfig.getMode());
        }
    }

    protected String prepareTemplates() {
        String singleSql;
        if (EWriteMode.INSERT.name().equalsIgnoreCase(jdbcConfig.getMode())) {
            singleSql = getInsertPrepareTemplate();
        } else if (EWriteMode.REPLACE.name().equalsIgnoreCase(jdbcConfig.getMode())) {
            singleSql = getReplacePrepareTemplate();
        } else if (EWriteMode.UPDATE.name().equalsIgnoreCase(jdbcConfig.getMode())) {
            singleSql = getUpsertStatement();
        } else {
            throw new IllegalArgumentException("Unknown write mode:" + jdbcConfig.getMode());
        }

        log.info("write sql:{}", singleSql);
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
            log.info(
                    "receive a convented ddlSql {} for table:{} and origin sql is {}",
                    ((DdlRowDataConvented) ddlRowData).getConventInfo(),
                    ddlRowData.getTableIdentifier().toString(),
                    ddlRowData.getSql());
        } else {
            log.info(
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
                        log.warn("execute sql {} error", finalSql, e);
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
            if (statementWrapper != null) {
                statementWrapper.close();
            }
        } catch (SQLException e) {
            log.error(ExceptionUtil.getErrorMessage(e));
        }
        JdbcUtil.closeDbResources(null, null, dbConn, true);
    }

    /**
     * 获取数据库连接，用于子类覆盖
     *
     * @return connection
     */
    protected Connection getConnection() throws SQLException {
        return JdbcUtil.getConnection(jdbcConfig, jdbcDialect);
    }

    protected void switchSchema(String schema, Statement statement) throws Exception {}

    public boolean tableExist(String catalogName, String schemaName, String tableName)
            throws SQLException {
        return dbConn.getMetaData()
                .getTables(catalogName, schemaName, tableName, new String[] {"TABLE"})
                .next();
    }

    public JdbcConfig getJdbcConfig() {
        return jdbcConfig;
    }

    public void setJdbcConf(JdbcConfig jdbcConfig) {
        this.jdbcConfig = jdbcConfig;
    }

    public void setJdbcDialect(JdbcDialect jdbcDialect) {
        this.jdbcDialect = jdbcDialect;
    }

    public void setColumnNameList(List<String> columnNameList) {
        this.columnNameList = columnNameList;
    }

    public void setColumnTypeList(List<TypeConfig> columnTypeList) {
        this.columnTypeList = columnTypeList;
    }

    public void setKeyRowType(RowType keyRowType) {
        this.keyRowType = keyRowType;
    }

    public void setKeyRowConverter(AbstractRowConverter keyRowConverter) {
        this.keyRowConverter = keyRowConverter;
    }
}
