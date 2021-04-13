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
package com.dtstack.flinkx.connector.jdbc.outputformat;

import org.apache.flink.connector.jdbc.statement.FieldNamedPreparedStatement;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.Row;

import com.dtstack.flinkx.conf.FieldConf;
import com.dtstack.flinkx.connector.jdbc.JdbcDialect;
import com.dtstack.flinkx.connector.jdbc.conf.JdbcConf;
import com.dtstack.flinkx.connector.jdbc.converter.AbstractJdbcRowConverter;
import com.dtstack.flinkx.connector.jdbc.util.JdbcUtil;
import com.dtstack.flinkx.enums.ColumnType;
import com.dtstack.flinkx.enums.EWriteMode;
import com.dtstack.flinkx.exception.WriteRecordException;
import com.dtstack.flinkx.outputformat.BaseRichOutputFormat;
import com.dtstack.flinkx.restore.FormatState;
import com.dtstack.flinkx.util.DateUtil;
import com.dtstack.flinkx.util.ExceptionUtil;
import com.dtstack.flinkx.util.GsonUtil;
import com.dtstack.flinkx.util.JsonUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

/**
 * OutputFormat for writing data to relational database.
 * <p>
 * Company: www.dtstack.com
 *
 * @author huyifan.zju@163.com
 */
public class JdbcOutputFormat extends BaseRichOutputFormat {

    protected static final Logger LOG = LoggerFactory.getLogger(JdbcOutputFormat.class);

    protected static final long serialVersionUID = 1L;

    protected static List<String> STRING_TYPES = Arrays.asList("CHAR", "VARCHAR", "VARCHAR2", "NVARCHAR2", "NVARCHAR", "TINYBLOB","TINYTEXT","BLOB","TEXT", "MEDIUMBLOB", "MEDIUMTEXT", "LONGBLOB", "LONGTEXT");
    protected JdbcConf jdbcConf;
    protected JdbcDialect jdbcDialect;
    protected AbstractJdbcRowConverter jdbcRowConverter;

    protected Connection dbConn;
    protected FieldNamedPreparedStatement fieldNamedPreparedStatement;
    protected List<String> column;
    protected List<String> columnType;
    protected List<String> fullColumnType;

    protected RowData lastRow = null;
    protected long rowsOfCurrentTransaction;
    protected List<String> fullColumn;

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

            //默认关闭事务自动提交，手动控制事务
            dbConn.setAutoCommit(false);

            Pair<List<String>, List<String>> pair = JdbcUtil.getTableFullColumns(getTableName(), dbConn);
            fullColumn = pair.getLeft();
            fullColumnType = pair.getRight();

            List<FieldConf> fieldList = jdbcConf.getColumn();
            if(fieldList.size() == 1 && Objects.equals(fieldList.get(0).getName(), "*")){
                column = fullColumn;
                columnType = fullColumnType;
            }else{
                column = new ArrayList<>(fieldList.size());
                columnType = new ArrayList<>(fieldList.size());
                for (FieldConf fieldConf : fieldList) {
                    column.add(fieldConf.getName());
                    for (int i = 0; i < fullColumn.size(); i++) {
                        if (fieldConf.getName().equalsIgnoreCase(fullColumn.get(i))) {
                            columnType.add(fullColumnType.get(i));
                            break;
                        }
                    }
                }
            }

            if (!EWriteMode.INSERT.name().equalsIgnoreCase(jdbcConf.getMode())) {
                List<String> updateKey = jdbcConf.getUpdateKey();
                if (CollectionUtils.isEmpty(updateKey)) {
                    List<String> tableIndex = JdbcUtil.getTableIndex(getTableName(), dbConn);
                    jdbcConf.setUpdateKey(tableIndex);
                    LOG.info("updateKey = {}", JsonUtil.toPrintJson(tableIndex));
                }
            }


            fieldNamedPreparedStatement = FieldNamedPreparedStatement.prepareStatement(
                    dbConn,
                    prepareTemplates(),
                    this.column.toArray(new String[0]));

            LOG.info("subTask[{}}] wait finished", taskNumber);
        } catch (SQLException sqe) {
            throw new IllegalArgumentException("open() failed.", sqe);
        } finally {
            JdbcUtil.commit(dbConn);
        }
    }

    @Override
    protected void writeSingleRecordInternal(RowData row) throws WriteRecordException {
        int index = 0;
        try {
            fieldNamedPreparedStatement = jdbcRowConverter.toExternal(row, this.fieldNamedPreparedStatement);
            fieldNamedPreparedStatement.execute();
            JdbcUtil.commit(dbConn);
        } catch (Exception e) {
            JdbcUtil.rollBack(dbConn);
            processWriteException(e, index, row);
        }
    }

    @Override
    protected String recordConvertDetailErrorMessage(int pos, RowData row) {
        return "\nJdbcOutputFormat [" + jobName + "] writeRecord error: when converting field["+ pos + "] in Row(" + row + ")";
    }

    @Override
    protected void writeMultipleRecordsInternal() throws Exception {
        try {
            for (RowData row : rows) {
                fieldNamedPreparedStatement = jdbcRowConverter.toExternal(row, this.fieldNamedPreparedStatement);
                fieldNamedPreparedStatement.executeBatch();
                lastRow = row;
            }

            if (checkpointEnabled) {
                rowsOfCurrentTransaction += rows.size();
            } else {
                //手动提交事务
                JdbcUtil.commit(dbConn);
            }
        } catch (Exception e) {
            LOG.warn("write Multiple Records error, row size = {}, first row = {},  e = {}",
                    rows.size(),
                    rows.size() > 0 ? GsonUtil.GSON.toJson(rows.get(0)) : "null",
                    ExceptionUtil.getErrorMessage(e));
            LOG.warn("error to writeMultipleRecords, start to rollback connection, e = {}", ExceptionUtil.getErrorMessage(e));
            JdbcUtil.rollBack(dbConn);
            throw e;
        } finally {
            //执行完后清空batch
            fieldNamedPreparedStatement.clearBatch();
        }
    }

    @Override
    public FormatState getFormatState() throws Exception {
        LOG.info("getFormatState:Start commit connection, rowsOfCurrentTransaction: {}", rowsOfCurrentTransaction);
        if (rows != null && rows.size() > 0) {
            super.writeRecordInternal();
        } else {
            fieldNamedPreparedStatement.executeBatch();
        }
        LOG.info("getFormatState:Commit connection success");

        if(jdbcConf.getRestoreColumnIndex() > -1){
            formatState.setState(((GenericRowData)lastRow).getField(jdbcConf.getRestoreColumnIndex()));
        }
        formatState.setNumberWrite(snapshotWriteCounter.getLocalValue());
        LOG.info("format state:{}", formatState.getState());
        super.getFormatState();
        return formatState;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        try {
            dbConn.commit();
            rowsOfCurrentTransaction = 0;
            snapshotWriteCounter.add(rowsOfCurrentTransaction);
            fieldNamedPreparedStatement.clearBatch();
        }catch (Exception e){
            dbConn.rollback();
            LOG.error("commit transaction error, e = {}", ExceptionUtil.getErrorMessage(e));
            throw e;
        }
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        dbConn.rollback();
    }

    /**
     * 执行pre、post SQL
     * @param sqlList
     */
    protected void executeBatch(List<String> sqlList) {
        if(CollectionUtils.isNotEmpty(sqlList)){
            try(Connection conn = getConnection();
                Statement stmt = conn.createStatement()
            ){
                for (String sql : sqlList) {
                    //兼容多条SQL写在同一行的情况
                    String[] strings = sql.split(";");
                    for (String s : strings) {
                        if(StringUtils.isNotBlank(s)){
                            stmt.addBatch(sql);
                        }
                    }
                }
            } catch (SQLException e) {
                LOG.error("execute preSql failed, preSql = {}, e = {}", JsonUtil.toPrintJson(sqlList), e);
            }
        }
    }

    protected String prepareTemplates() {
        String singleSql;
        if (EWriteMode.INSERT.name().equalsIgnoreCase(jdbcConf.getMode())) {
            singleSql = jdbcDialect.getInsertIntoStatement(getTableName(), column.toArray(new String[0]));
        } else if (EWriteMode.REPLACE.name().equalsIgnoreCase(jdbcConf.getMode())) {
            singleSql = jdbcDialect.getReplaceStatement(getTableName(), column.toArray(new String[0])).get();
        } else if (EWriteMode.UPDATE.name().equalsIgnoreCase(jdbcConf.getMode())) {
            singleSql = jdbcDialect.getUpsertStatement(getTableName(), column.toArray(new String[0]), jdbcConf.getUpdateKey().toArray(new String[0]), jdbcConf.isAllReplace()).get();
        } else {
            throw new IllegalArgumentException("Unknown write mode:" + jdbcConf.getMode());
        }

        LOG.info("write sql:{}", singleSql);
        return singleSql;
    }

    protected void processWriteException(
            Exception e,
            int index,
            RowData row) throws WriteRecordException {
        if (e instanceof SQLException) {
            if (e.getMessage().contains("No operations allowed")) {
                throw new RuntimeException("Connection maybe closed", e);
            }
        }

        if (index < row.getArity()) {
            String message = recordConvertDetailErrorMessage(index, row);
            LOG.error(message, e);
            throw new WriteRecordException(message, e, index, row);
        }
        throw new WriteRecordException(e.getMessage(), e);
    }

    /**
     * 获取转换后的字段value
     *
     * todo 迁移到插件中
     *
     * @param row
     * @param index
     *
     * @return
     */
    protected Object getField(Row row, int index) {
        Object field = row.getField(index);
        String type = columnType.get(index);

        //field为空字符串，且写入目标类型不为字符串类型的字段，则将object设置为null
        if (field instanceof String
                && StringUtils.isBlank((String) field)
                && !STRING_TYPES.contains(type.toUpperCase(Locale.ENGLISH))) {
            return null;
        }

        if (type.matches(DateUtil.DATE_REGEX)) {
            field = DateUtil.columnToDate(field, null);
        } else if (type.matches(DateUtil.DATETIME_REGEX)
                || type.matches(DateUtil.TIMESTAMP_REGEX)) {
            field = DateUtil.columnToTimestamp(field, null);
        }

        if (type.equalsIgnoreCase(ColumnType.BIGINT.name()) && field instanceof java.util.Date) {
            field = ((java.util.Date) field).getTime();
        }

        return field;
    }

    @Override
    public void closeInternal() {
        numWriteCounter.add(rowsOfCurrentTransaction);
        try {
            if(fieldNamedPreparedStatement != null){
                fieldNamedPreparedStatement.close();
            }
        } catch (SQLException e) {
            LOG.error(ExceptionUtil.getErrorMessage(e));
        }
        JdbcUtil.closeDbResources(null, null, dbConn, true);
    }

    /**
     * 获取table名称，如果table是schema.table格式，可重写此方法 只返回table
     *
     * @return
     */
    protected String getTableName() {
        return jdbcConf.getTable();
    }

    /**
     * 获取数据库连接，用于子类覆盖
     * @return connection
     */
    protected Connection getConnection() {
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

    public void setJdbcRowConverter(AbstractJdbcRowConverter jdbcRowConverter) {
        this.jdbcRowConverter = jdbcRowConverter;
    }
}
