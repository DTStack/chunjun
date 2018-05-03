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

import com.dtstack.flinkx.exception.WriteRecordException;
import com.dtstack.flinkx.rdb.DatabaseInterface;
import com.dtstack.flinkx.rdb.util.DBUtil;
import com.dtstack.flinkx.outputformat.RichOutputFormat;
import com.dtstack.flinkx.util.ClassUtil;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Company: www.dtstack.com
 * @author sishu.yss
 */
public class JdbcOutputFormat extends RichOutputFormat {

    protected static final long serialVersionUID = 1L;

    protected static final Logger LOG = LoggerFactory.getLogger(JdbcOutputFormat.class);

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

    protected String mode = "insert";

    protected String table;

    protected List<String> column;

    protected Map<String,List<String>> updateKey;

    protected List<String> fullColumn;



    protected PreparedStatement prepareSingleTemplates() throws SQLException {
        String singleSql = null;
        if (mode == null || mode.length() == 0 || mode.equalsIgnoreCase("INSERT")) {
            singleSql = databaseInterface.getInsertStatement(column, table);
        } else if (mode.equalsIgnoreCase("REPLACE")) {
            if(fullColumn == null || fullColumn.size() == 0) {

            }
            singleSql = databaseInterface.getReplaceStatement(column, fullColumn, table, updateKey);
        } else if (mode.equalsIgnoreCase("UPDATE")) {
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
        String multipleSql = null;
        if (mode == null || mode.length() == 0 || mode.equalsIgnoreCase("INSERT")) {
            multipleSql = databaseInterface.getMultiInsertStatement(column, table, batchSize);
        } else if (mode.equalsIgnoreCase("REPLACE")) {
            multipleSql = databaseInterface.getMultiReplaceStatement(column, fullColumn, table, batchSize, updateKey);
        } else if (mode.equalsIgnoreCase("UPDATE")) {
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

            if(fullColumn == null || fullColumn.size() == 0) {
                fullColumn = probeFullColumns(table, dbConn);
            }

            if(updateKey == null || updateKey.size() == 0) {
                updateKey = probePrimaryKeys(table, dbConn);
            }

            singleUpload = prepareSingleTemplates();
            multipleUpload = prepareMultipleTemplates();

            LOG.info("subtask[" + taskNumber + "] wait finished");
        } catch (SQLException sqe) {
            throw new IllegalArgumentException("open() failed.", sqe);
        }
    }

    @Override
    protected void writeSingleRecordInternal(Row row) throws WriteRecordException {
        int index = 0;
        try {
            for (; index < row.getArity(); index++) {
                Object field = getField(row, index);
                singleUpload.setObject(index + 1, field);
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
        if(rows.size() == batchInterval) {
            upload = multipleUpload;
        } else {
            upload = prepareMultipleTemplates(rows.size());
        }

        int k = 1;
        for(int i = 0; i < rows.size(); ++i) {
            Row row = rows.get(i);
            for(int j = 0; j < row.getArity(); ++j) {
                Object field = getField(row, j);
                upload.setObject(k, field);
                k++;
            }
        }

        upload.execute();
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
        List<String> ret = new ArrayList<>();
        ResultSet rs = dbConn.getMetaData().getColumns(null, null, table, null);
        while(rs.next()) {
            ret.add(rs.getString("COLUMN_NAME"));
        }
        return ret;
    }

    protected Map<String, List<String>> probePrimaryKeys(String table, Connection dbConn) throws SQLException {
        Map<String, List<String>> map = new HashMap<>();
        ResultSet rs = dbConn.getMetaData().getIndexInfo(null, null, table, true, false);
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
        return  preSql != null;
    }

    @Override
    protected void beforeWriteRecords()  {
        if(taskNumber == 1) {
            DBUtil.executeBatch(dbConn, preSql);
        }
    }

    @Override
    protected boolean needWaitBeforeCloseInternal() {
        return postSql != null;
    }

    @Override
    protected void beforeCloseInternal() {
        // 执行postsql
        if(taskNumber ==1) {
            DBUtil.executeBatch(dbConn, postSql);
        }
    }


}

