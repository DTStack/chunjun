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
package com.dtstack.flinkx.rdb.outputformat;

import com.dtstack.flinkx.enums.ColumnType;
import com.dtstack.flinkx.enums.EWriteMode;
import com.dtstack.flinkx.exception.WriteRecordException;
import com.dtstack.flinkx.outputformat.BaseRichOutputFormat;
import com.dtstack.flinkx.rdb.DatabaseInterface;
import com.dtstack.flinkx.rdb.type.TypeConverterInterface;
import com.dtstack.flinkx.rdb.util.DbUtil;
import com.dtstack.flinkx.restore.FormatState;
import com.dtstack.flinkx.util.ClassUtil;
import com.dtstack.flinkx.util.DateUtil;
import com.google.gson.Gson;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.ObjectUtils;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * OutputFormat for writing data to relational database.
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class JdbcOutputFormat extends BaseRichOutputFormat {

    protected static final Logger LOG = LoggerFactory.getLogger(JdbcOutputFormat.class);

    protected static final long serialVersionUID = 1L;

    protected String username;

    protected String password;

    protected String driverName;

    protected String dbUrl;

    protected Connection dbConn;

    protected PreparedStatement preparedStatement;

    protected List<String> preSql;

    protected List<String> postSql;

    protected DatabaseInterface databaseInterface;

    protected String mode = EWriteMode.INSERT.name();

    /**just for postgresql,use copy replace insert*/
    protected String insertSqlMode;

    protected String table;

    protected List<String> column;

    protected Map<String,List<String>> updateKey;

    protected List<String> fullColumn;

    protected List<String> fullColumnType;

    protected List<String> columnType = new ArrayList<>();

    protected TypeConverterInterface typeConverter;

    protected Row lastRow = null;

    protected boolean readyCheckpoint;

    protected long rowsOfCurrentTransaction;

    protected final static String GET_INDEX_SQL = "SELECT " +
            "t.INDEX_NAME," +
            "t.COLUMN_NAME " +
            "FROM " +
            "user_ind_columns t," +
            "user_indexes i " +
            "WHERE " +
            "t.index_name = i.index_name " +
            "AND i.uniqueness = 'UNIQUE' " +
            "AND t.table_name = '%s'";

    protected final static String CONN_CLOSE_ERROR_MSG = "No operations allowed";

    protected PreparedStatement prepareTemplates() throws SQLException {
        if(CollectionUtils.isEmpty(fullColumn)) {
            fullColumn = column;
        }

        String singleSql;
        if (EWriteMode.INSERT.name().equalsIgnoreCase(mode)) {
            singleSql = databaseInterface.getInsertStatement(column, table);
        } else if (EWriteMode.REPLACE.name().equalsIgnoreCase(mode)) {
            singleSql = databaseInterface.getReplaceStatement(column, fullColumn, table, updateKey);
        } else if (EWriteMode.UPDATE.name().equalsIgnoreCase(mode)) {
            singleSql = databaseInterface.getUpsertStatement(column, table, updateKey);
        } else {
            throw new IllegalArgumentException("Unknown write mode:" + mode);
        }

        LOG.info("write sql:{}", singleSql);

        return dbConn.prepareStatement(singleSql);
    }

    @Override
    protected void openInternal(int taskNumber, int numTasks){
        try {
            ClassUtil.forName(driverName, getClass().getClassLoader());
            dbConn = DbUtil.getConnection(dbUrl, username, password);

            if (restoreConfig.isRestore()){
                dbConn.setAutoCommit(false);
            }

            if(CollectionUtils.isEmpty(fullColumn)) {
                fullColumn = probeFullColumns(table, dbConn);
            }

            if (!EWriteMode.INSERT.name().equalsIgnoreCase(mode)){
                if(updateKey == null || updateKey.size() == 0) {
                    updateKey = probePrimaryKeys(table, dbConn);
                }
            }

            if(fullColumnType == null) {
                fullColumnType = analyzeTable();
            }

            for(String col : column) {
                for (int i = 0; i < fullColumn.size(); i++) {
                    if (col.equalsIgnoreCase(fullColumn.get(i))){
                        columnType.add(fullColumnType.get(i));
                        break;
                    }
                }
            }

            preparedStatement = prepareTemplates();
            readyCheckpoint = false;

            LOG.info("subTask[{}}] wait finished", taskNumber);
        } catch (SQLException sqe) {
            throw new IllegalArgumentException("open() failed.", sqe);
        }
    }

    protected List<String> analyzeTable() {
        List<String> ret = new ArrayList<>();
        Statement stmt = null;
        ResultSet rs = null;
        try {
            stmt = dbConn.createStatement();
            rs = stmt.executeQuery(databaseInterface.getSqlQueryFields(databaseInterface.quoteTable(table)));
            ResultSetMetaData rd = rs.getMetaData();
            for(int i = 0; i < rd.getColumnCount(); ++i) {
                ret.add(rd.getColumnTypeName(i+1));
            }

            if(CollectionUtils.isEmpty(fullColumn)){
                for(int i = 0; i < rd.getColumnCount(); ++i) {
                    fullColumn.add(rd.getColumnName(i+1));
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            DbUtil.closeDbResources(rs, stmt,null, false);
        }

        return ret;
    }

    @Override
    protected void writeSingleRecordInternal(Row row) throws WriteRecordException {
        int index = 0;
        try {
            for (; index < row.getArity(); index++) {
                preparedStatement.setObject(index+1,getField(row,index));
            }

            preparedStatement.execute();
        } catch (Exception e) {
            processWriteException(e, index, row);
        }
    }

    protected void processWriteException(Exception e, int index, Row row) throws WriteRecordException{
        if(e instanceof SQLException){
            if(e.getMessage().contains(CONN_CLOSE_ERROR_MSG)){
                throw new RuntimeException("Connection maybe closed", e);
            }
        }

        if(index < row.getArity()) {
            throw new WriteRecordException(recordConvertDetailErrorMessage(index, row), e, index, row);
        }
        throw new WriteRecordException(e.getMessage(), e);
    }

    @Override
    protected String recordConvertDetailErrorMessage(int pos, Row row) {
        return "\nJdbcOutputFormat [" + jobName + "] writeRecord error: when converting field[" + pos + "] in Row(" + row + ")";
    }

    @Override
    protected void writeMultipleRecordsInternal() throws Exception {
        try {
            for (Row row : rows) {
                for (int j = 0; j < row.getArity(); ++j) {
                    preparedStatement.setObject(j + 1, getField(row, j));
                }
                preparedStatement.addBatch();

                if (restoreConfig.isRestore()) {
                    if (lastRow != null){
                        readyCheckpoint = !ObjectUtils.equals(lastRow.getField(restoreConfig.getRestoreColumnIndex()),
                                row.getField(restoreConfig.getRestoreColumnIndex()));
                    }

                    lastRow = row;
                }
            }

            preparedStatement.executeBatch();

            if(restoreConfig.isRestore()){
                rowsOfCurrentTransaction += rows.size();
            }
        } catch (Exception e){
            if (restoreConfig.isRestore()){
                LOG.warn("writeMultipleRecordsInternal:Start rollback");
                dbConn.rollback();
                LOG.warn("writeMultipleRecordsInternal:Rollback success");
            }

            throw e;
        }
    }

    @Override
    public FormatState getFormatState(){
        if (!restoreConfig.isRestore() || lastRow == null){
            LOG.info("return null for formatState");
            return null;
        }

        try {
            LOG.info("readyCheckpoint: {}, rowsOfCurrentTransaction: {}", readyCheckpoint, rowsOfCurrentTransaction);

            if (readyCheckpoint || rowsOfCurrentTransaction > restoreConfig.getMaxRowNumForCheckpoint()){

                LOG.info("getFormatState:Start commit connection");
                if(rows != null && rows.size() > 0){
                    super.writeRecordInternal();
                }else{
                    preparedStatement.executeBatch();
                }
                dbConn.commit();
                LOG.info("getFormatState:Commit connection success");

                snapshotWriteCounter.add(rowsOfCurrentTransaction);
                numWriteCounter.add(rowsOfCurrentTransaction);
                rowsOfCurrentTransaction = 0;

                formatState.setState(lastRow.getField(restoreConfig.getRestoreColumnIndex()));
                formatState.setNumberWrite(snapshotWriteCounter.getLocalValue());
                LOG.info("format state:{}", formatState.getState());

                super.getFormatState();
                return formatState;
            }

            return null;
        } catch (Exception e){
            try {
                LOG.warn("getFormatState:Start rollback");
                dbConn.rollback();
                LOG.warn("getFormatState:Rollback success");
            } catch (SQLException sqlE){
                throw new RuntimeException("Rollback error:", e);
            }

            throw new RuntimeException("Return format state error:", e);
        }
    }

    protected Object getField(Row row, int index) {
        Object field = row.getField(index);
        String type = columnType.get(index);
        if(type.matches(DateUtil.DATE_REGEX)) {
            field = DateUtil.columnToDate(field,null);
        } else if(type.matches(DateUtil.DATETIME_REGEX) || type.matches(DateUtil.TIMESTAMP_REGEX)){
            field = DateUtil.columnToTimestamp(field,null);
        }

        if (type.equalsIgnoreCase(ColumnType.BIGINT.name()) && field instanceof java.util.Date){
            field = ((java.util.Date) field).getTime();
        }

        return field;
    }

    protected List<String> probeFullColumns(String table, Connection dbConn) throws SQLException {
        List<String> ret = new ArrayList<>();
        ResultSet rs = dbConn.getMetaData().getColumns(null, null, table, null);
        while(rs.next()) {
            ret.add(rs.getString("COLUMN_NAME"));
        }
        return ret;
    }

    protected Map<String, List<String>> probePrimaryKeys(String table, Connection dbConn) throws SQLException {
        Map<String, List<String>> map = new HashMap<>(16);
        ResultSet rs = dbConn.getMetaData().getIndexInfo(null, null, table, true, false);
        while(rs.next()) {
            String indexName = rs.getString("INDEX_NAME");
            if(!map.containsKey(indexName)) {
                map.put(indexName,new ArrayList<>());
            }
            map.get(indexName).add(rs.getString("COLUMN_NAME"));
        }
        Map<String,List<String>> retMap = new HashMap<>((map.size()<<2)/3);
        for(Map.Entry<String,List<String>> entry: map.entrySet()) {
            String k = entry.getKey();
            List<String> v = entry.getValue();
            if(v!=null && v.size() != 0 && v.get(0) != null) {
                retMap.put(k, v);
            }
        }
        return retMap;
    }

    @Override
    public void closeInternal() {
        readyCheckpoint = false;
        boolean commit = true;
        try{
            numWriteCounter.add(rowsOfCurrentTransaction);
            String state = getTaskState();
            // Do not commit a transaction when the task is canceled or failed
            if(!RUNNING_STATE.equals(state) && restoreConfig.isRestore()){
                commit = false;
            }
        } catch (Exception e){
            LOG.error("Get task status error:{}", e.getMessage());
        }

        DbUtil.closeDbResources(null, preparedStatement, dbConn, commit);
        dbConn = null;
    }

    @Override
    protected boolean needWaitBeforeWriteRecords() {
        return  CollectionUtils.isNotEmpty(preSql);
    }

    @Override
    protected void beforeWriteRecords()  {
        if(taskNumber == 0) {
            LOG.info("start to execute preSql, preSql = {}", new Gson().toJson(preSql));
            DbUtil.executeBatch(dbConn, preSql);
        }
    }

    @Override
    protected boolean needWaitBeforeCloseInternal() {
        return  CollectionUtils.isNotEmpty(postSql);
    }

    @Override
    protected void beforeCloseInternal() {
        // 执行postsql
        if(taskNumber == 0) {
            LOG.info("start to execute postSql, postSql = {}", new Gson().toJson(postSql));
            DbUtil.executeBatch(dbConn, postSql);
        }
    }

}
