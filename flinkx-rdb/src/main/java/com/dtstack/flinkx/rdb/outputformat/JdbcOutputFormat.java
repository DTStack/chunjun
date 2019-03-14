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

import com.dtstack.flinkx.enums.ColType;
import com.dtstack.flinkx.enums.EDatabaseType;
import com.dtstack.flinkx.enums.EWriteMode;
import com.dtstack.flinkx.exception.WriteRecordException;
import com.dtstack.flinkx.outputformat.RichOutputFormat;
import com.dtstack.flinkx.rdb.DatabaseInterface;
import com.dtstack.flinkx.rdb.type.TypeConverterInterface;
import com.dtstack.flinkx.rdb.util.DBUtil;
import com.dtstack.flinkx.util.ClassUtil;
import com.dtstack.flinkx.util.DateUtil;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;
import java.text.SimpleDateFormat;
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
public class JdbcOutputFormat extends RichOutputFormat {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcOutputFormat.class);

    protected static final long serialVersionUID = 1L;

    protected String username;

    protected String password;

    protected String driverName;

    protected String dbURL;

    protected Connection dbConn;

    protected PreparedStatement preparedStatement;

    protected List<String> preSql;

    protected List<String> postSql;

    protected DatabaseInterface databaseInterface;

    protected String mode = EWriteMode.INSERT.name();

    protected String table;

    protected List<String> column;

    protected Map<String,List<String>> updateKey;

    protected List<String> fullColumn;

    protected List<String> fullColumnType;

    private List<String> columnType = new ArrayList<>();

    protected TypeConverterInterface typeConverter;

    private final static String GET_ORACLE_INDEX_SQL = "SELECT " +
            "t.INDEX_NAME," +
            "t.COLUMN_NAME " +
            "FROM " +
            "user_ind_columns t," +
            "user_indexes i " +
            "WHERE " +
            "t.index_name = i.index_name " +
            "AND i.uniqueness = 'UNIQUE' " +
            "AND t.table_name = '%s'";

    protected PreparedStatement prepareTemplates() throws SQLException {
        if(fullColumn == null || fullColumn.size() == 0) {
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

        return dbConn.prepareStatement(singleSql);
    }

    @Override
    protected void openInternal(int taskNumber, int numTasks) throws IOException {
        try {
            ClassUtil.forName(driverName, getClass().getClassLoader());
            dbConn = DBUtil.getConnection(dbURL, username, password);

            if(fullColumn == null || fullColumn.size() == 0) {
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

            LOG.info("subtask[" + taskNumber + "] wait finished");
        } catch (SQLException sqe) {
            throw new IllegalArgumentException("open() failed.", sqe);
        }
    }

    private List<String> analyzeTable() {
        List<String> ret = new ArrayList<>();
        Statement stmt = null;
        ResultSet rs = null;
        try {
            stmt = dbConn.createStatement();
            rs = stmt.executeQuery(databaseInterface.getSQLQueryFields(databaseInterface.quoteTable(table)));
            ResultSetMetaData rd = rs.getMetaData();
            for(int i = 0; i < rd.getColumnCount(); ++i) {
                ret.add(rd.getColumnTypeName(i+1));
            }

            if(fullColumn == null || fullColumn.size() == 0){
                for(int i = 0; i < rd.getColumnCount(); ++i) {
                    fullColumn.add(rd.getColumnName(i+1));
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            DBUtil.closeDBResources(rs,stmt,null);
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
            if(index < row.getArity()) {
                throw new WriteRecordException(recordConvertDetailErrorMessage(index, row), e, index, row);
            }
            throw new WriteRecordException(e.getMessage(), e);
        }
    }

    @Override
    protected String recordConvertDetailErrorMessage(int pos, Row row) {
        return "\nJdbcOutputFormat [" + jobName + "] writeRecord error: when converting field[" + pos + "] in Row(" + row + ")";
    }

    @Override
    protected void writeMultipleRecordsInternal() throws Exception {
        for(int i = 0; i < rows.size(); ++i) {
            Row row = rows.get(i);
            for(int j = 0; j < row.getArity(); ++j) {
                preparedStatement.setObject(j + 1, getField(row, j));
            }

            preparedStatement.addBatch();
        }

        preparedStatement.executeBatch();
    }

    protected Object getField(Row row, int index) {
        Object field = row.getField(index);
        String type = columnType.get(index);
        if(type.matches(DateUtil.DATE_REGEX)) {
            field = DateUtil.columnToDate(field,null);
        } else if(type.matches(DateUtil.DATETIME_REGEX) || type.matches(DateUtil.TIMESTAMP_REGEX)){
            field = DateUtil.columnToTimestamp(field,null);
        }

        if (type.equalsIgnoreCase(ColType.BIGINT.toString()) && field instanceof java.util.Date){
            field = ((java.util.Date) field).getTime();
        }

        field=dealOracleTimestampToVarcharOrLong(databaseInterface.getDatabaseType(),field,type);


        if(EDatabaseType.PostgreSQL == databaseInterface.getDatabaseType()){
            field = typeConverter.convert(field,type);
        }

        return field;
    }

    /**
     * oracle timestamp to oracle varchar or varchar2 or long field format
     * @param databaseType
     * @param field
     * @param type
     * @return
     */
    private Object dealOracleTimestampToVarcharOrLong(EDatabaseType databaseType, Object field, String type) {
        if (EDatabaseType.Oracle!=databaseInterface.getDatabaseType()){
            return field;
        }

        if (!(field instanceof Timestamp)){
            return field;
        }

        if (type.equalsIgnoreCase(ColType.VARCHAR.toString()) || type.equalsIgnoreCase(ColType.VARCHAR2.toString())){
            SimpleDateFormat format = DateUtil.getDateTimeFormatter();
            field= format.format(field);
        }

        if (type.equalsIgnoreCase(ColType.LONG.toString()) ){
            field = ((Timestamp) field).getTime();
        }
        return field;
    }

    protected List<String> probeFullColumns(String table, Connection dbConn) throws SQLException {
        String schema =null;
        if(EDatabaseType.Oracle == databaseInterface.getDatabaseType()) {
            String[] parts = table.split("\\.");
            if(parts.length == 2) {
                schema = parts[0].toUpperCase();
                table = parts[1];
            }
        }

        List<String> ret = new ArrayList<>();
        ResultSet rs = dbConn.getMetaData().getColumns(null, schema, table, null);
        while(rs.next()) {
            ret.add(rs.getString("COLUMN_NAME"));
        }
        return ret;
    }



    protected Map<String, List<String>> probePrimaryKeys(String table, Connection dbConn) throws SQLException {
        Map<String, List<String>> map = new HashMap<>();
        ResultSet rs;
        if(EDatabaseType.Oracle == databaseInterface.getDatabaseType()){
            PreparedStatement ps = dbConn.prepareStatement(String.format(GET_ORACLE_INDEX_SQL,table));
            rs = ps.executeQuery();
        } else if(EDatabaseType.DB2 == databaseInterface.getDatabaseType()){
            rs = dbConn.getMetaData().getIndexInfo(null, null, table.toUpperCase(), true, false);
        } else {
            rs = dbConn.getMetaData().getIndexInfo(null, null, table, true, false);
        }

        while(rs.next()) {
            String indexName = rs.getString("INDEX_NAME");
            if(!map.containsKey(indexName)) {
                map.put(indexName,new ArrayList<>());
            }
            map.get(indexName).add(rs.getString("COLUMN_NAME"));
        }
        Map<String,List<String>> retMap = new HashMap<>();
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
        DBUtil.closeDBResources(null, preparedStatement, dbConn);
        dbConn = null;

        //FIXME TEST
        //oracle
        if (EDatabaseType.Oracle == databaseInterface.getDatabaseType()) {
            String oracleTimeoutPollingThreadName = "OracleTimeoutPollingThread";
            Thread thread = getThreadByName(oracleTimeoutPollingThreadName);
            if(thread != null){
                thread.interrupt();
                LOG.warn("----close curr oracle polling thread: " + oracleTimeoutPollingThreadName);
            }
        }
    }

    public Thread getThreadByName(String name){
        for(Thread t : Thread.getAllStackTraces().keySet()){
            if(t.getName().equals(name)){
                return t;
            }
        }

        return null;
    }

    @Override
    protected boolean needWaitBeforeWriteRecords() {
        return  preSql != null && preSql.size() != 0;
    }

    @Override
    protected void beforeWriteRecords()  {
        if(taskNumber == 0) {
            DBUtil.executeBatch(dbConn, preSql);
        }
    }

    @Override
    protected boolean needWaitBeforeCloseInternal() {
        return postSql != null && postSql.size() != 0;
    }

    @Override
    protected void beforeCloseInternal() {
        // 执行postsql
        if(taskNumber == 0) {
            DBUtil.executeBatch(dbConn, postSql);
        }
    }

}
