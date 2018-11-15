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

import com.dtstack.flinkx.enums.EDatabaseType;
import com.dtstack.flinkx.enums.EWriteMode;
import com.dtstack.flinkx.exception.WriteRecordException;
import com.dtstack.flinkx.rdb.DatabaseInterface;
import com.dtstack.flinkx.rdb.type.TypeConverterInterface;
import com.dtstack.flinkx.rdb.util.DBUtil;
import com.dtstack.flinkx.outputformat.RichOutputFormat;
import com.dtstack.flinkx.util.ClassUtil;
import com.dtstack.flinkx.util.DateUtil;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.*;

/**
 * OutputFormat for writing data to relational database.
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class JdbcOutputFormat extends RichOutputFormat {

    protected static final long serialVersionUID = 1L;

    protected String username;

    protected String password;

    protected String drivername;

    protected String dbURL;

    protected Connection dbConn;

    protected PreparedStatement singleUpload;

    protected PreparedStatement multipleUpload;

    protected int taskNumber;

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

    private final static String DATE_REGEX = "(?i)date";

    private final static String TIMESTAMP_REGEX = "(?i)timestamp";

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

    protected PreparedStatement prepareSingleTemplates() throws SQLException {
        if(fullColumn == null || fullColumn.size() == 0) {
            fullColumn = column;
        }

        String singleSql = null;
        if (EWriteMode.INSERT.name().equalsIgnoreCase(mode)) {
            singleSql = databaseInterface.getInsertStatement(column, table);
        } else if (EWriteMode.REPLACE.name().equalsIgnoreCase(mode)) {
            singleSql = databaseInterface.getReplaceStatement(column, fullColumn, table, updateKey);
        } else if (EWriteMode.UPDATE.name().equalsIgnoreCase(mode)) {
            singleSql = databaseInterface.getUpsertStatement(column, table, updateKey);
        } else {
            throw new IllegalArgumentException();
        }
        return dbConn.prepareStatement(singleSql);
    }

    protected PreparedStatement prepareMultipleTemplates() throws SQLException {
        return prepareMultipleTemplates(batchInterval);
    }

    protected PreparedStatement prepareMultipleTemplates(int batchSize) throws SQLException {
        if(fullColumn == null || fullColumn.size() == 0) {
            fullColumn = column;
        }

        String multipleSql = null;
        if (EWriteMode.INSERT.name().equalsIgnoreCase(mode)) {
            multipleSql = databaseInterface.getMultiInsertStatement(column, table, batchSize);
        } else if (EWriteMode.REPLACE.name().equalsIgnoreCase(mode)) {
            multipleSql = databaseInterface.getMultiReplaceStatement(column, fullColumn, table, batchSize, updateKey);
        } else if (EWriteMode.UPDATE.name().equalsIgnoreCase(mode)) {
            multipleSql = databaseInterface.getMultiUpsertStatement(column, table, batchSize, updateKey);
        } else {
            throw new IllegalArgumentException();
        }
        return dbConn.prepareStatement(multipleSql);
    }

    @Override
    protected void openInternal(int taskNumber, int numTasks) throws IOException {
        try {
            ClassUtil.forName(drivername, getClass().getClassLoader());
            dbConn = DBUtil.getConnection(dbURL, username, password);

            if(batchInterval > 1 && databaseInterface.getDatabaseType() != EDatabaseType.Oracle){
                dbConn.setAutoCommit(false);
            }

            if(fullColumn == null || fullColumn.size() == 0) {
                fullColumn = probeFullColumns(table, dbConn);
            }

            if (!EWriteMode.INSERT.name().equalsIgnoreCase(mode)){
                if(updateKey == null || updateKey.size() == 0) {
                    updateKey = probePrimaryKeys(table, dbConn);
                }
            }

            singleUpload = prepareSingleTemplates();
            multipleUpload = prepareMultipleTemplates();

            if(fullColumnType == null) {
                fullColumnType = analyzeTable();
            }

            for(String col : column) {
                columnType.add(fullColumnType.get(fullColumn.indexOf(col)));
            }

            LOG.info("subtask[" + taskNumber + "] wait finished");
        } catch (SQLException sqe) {
            throw new IllegalArgumentException("open() failed.", sqe);
        }
    }

    private List<String> analyzeTable() {
        List<String> ret = new ArrayList<>();

        try {
            Statement stmt = dbConn.createStatement();
            ResultSet rs = stmt.executeQuery(databaseInterface.getSQLQueryFields(databaseInterface.quoteTable(table)));
            ResultSetMetaData rd = rs.getMetaData();
            for(int i = 0; i < rd.getColumnCount(); ++i) {
                ret.add(rd.getColumnTypeName(i+1));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return ret;
    }

    private Object convertField(Row row, int index) {
        Object field = getField(row, index);
        if(EDatabaseType.Oracle == databaseInterface.getDatabaseType()) {
            String type = columnType.get(index);
            if(type.equalsIgnoreCase("DATE")) {
                field = DateUtil.columnToDate(field);
            } else if(type.equalsIgnoreCase("TIMESTAMP")){
                field = DateUtil.columnToTimestamp(field);
            }
        } else if(EDatabaseType.PostgreSQL == databaseInterface.getDatabaseType()){
            if(columnType != null && columnType.size() != 0) {
                field = typeConverter.convert(field,columnType.get(index));
            }
        }
        return field;
    }

    @Override
    protected void writeSingleRecordInternal(Row row) throws WriteRecordException {
        int index = 0;
        try {
            for (; index < row.getArity(); index++) {
                String type = columnType.get(index);
                fillUploadStmt(singleUpload, index+1, convertField(row, index), type);
            }
            singleUpload.execute();
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
        PreparedStatement upload;
        if(rows.size() == batchInterval && EDatabaseType.SQLServer != databaseInterface.getDatabaseType()) {
            upload = multipleUpload;
        } else {
            upload = prepareMultipleTemplates(rows.size());
        }

        int k = 1;
        for(int i = 0; i < rows.size(); ++i) {
            Row row = rows.get(i);
            for(int j = 0; j < row.getArity(); ++j) {
                String type = columnType.get(j);
                fillUploadStmt(upload, k, convertField(row, j), type);
                k++;
            }
        }

        upload.execute();
        if(databaseInterface.getDatabaseType() != EDatabaseType.Oracle){
            dbConn.commit();
        }
    }

    private void fillUploadStmt(PreparedStatement upload, int k, Object field, String type) throws SQLException {
        if(type.matches(DATE_REGEX)) {
            if (field instanceof Timestamp){
                field = new java.sql.Date(((Timestamp) field).getTime());
            }
            upload.setDate(k, (java.sql.Date) field);
        } else if(type.matches(TIMESTAMP_REGEX)) {
            upload.setTimestamp(k, (Timestamp) field);
        } else {
            upload.setObject(k, field);
        }
    }

    protected Object getField(Row row, int index) {
        Object field = row.getField(index);
        if (field != null && field.getClass() == java.util.Date.class) {
            java.util.Date d = (java.util.Date) field;
            field = new Timestamp(d.getTime());
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
        if(taskNumber != 0) {
            DBUtil.closeDBResources(null,null,dbConn);
            dbConn = null;
        }
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

