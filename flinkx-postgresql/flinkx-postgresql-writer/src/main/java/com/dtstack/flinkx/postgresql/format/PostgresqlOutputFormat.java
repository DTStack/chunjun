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
package com.dtstack.flinkx.postgresql.format;

import com.dtstack.flinkx.enums.EWriteMode;
import com.dtstack.flinkx.exception.WriteRecordException;
import com.dtstack.flinkx.rdb.outputformat.JdbcOutputFormat;
import org.apache.flink.types.Row;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * when  postgresql with mode insert, it use 'copy tableName(columnName) from stdin' syntax
 * Date: 2019/8/5
 * Company: www.dtstack.com
 * @author xuchao
 */

public class PostgresqlOutputFormat extends JdbcOutputFormat {

    private static final String COPY_SQL_TEMPL = "copy %s(%s) from stdin DELIMITER '%s'";

    private static final String DEFAULT_FIELD_DELIM = "\001";

    private static final String LINE_DELIMITER = "\n";

    /**
     * now just add ext insert mode:copy
     */
    private static final String INSERT_SQL_MODE_TYPE = "copy";

    private String copySql = "";

    private CopyManager copyManager;


    @Override
    protected PreparedStatement prepareTemplates() throws SQLException {
        if(fullColumn == null || fullColumn.size() == 0) {
            fullColumn = column;
        }

        //check is use copy mode for insert
        if (EWriteMode.INSERT.name().equalsIgnoreCase(mode) && checkIsCopyMode(insertSqlMode)) {
            copyManager = new CopyManager((BaseConnection) dbConn);
            copySql = String.format(COPY_SQL_TEMPL, table, String.join(",", column), DEFAULT_FIELD_DELIM);
            return null;
        }

        return super.prepareTemplates();
    }

    @Override
    protected void openInternal(int taskNumber, int numTasks){
        super.openInternal(taskNumber, numTasks);
        try {
            if (batchInterval > 1) {
                dbConn.setAutoCommit(false);
            }
        } catch (Exception e) {
            LOG.warn("", e);
        }
    }

    @Override
    protected void writeSingleRecordInternal(Row row) throws WriteRecordException {
        if(!checkIsCopyMode(insertSqlMode)){
            if (batchInterval == 1) {
                super.writeSingleRecordInternal(row);
            } else {
                writeSingleRecordCommit(row);
            }

            return;
        }

        //write with copy
        int index = 0;
        try {
            StringBuilder sb = new StringBuilder();
            for (; index < row.getArity(); index++) {
                Object rowData = getField(row, index);
                sb.append(rowData)
                        .append(DEFAULT_FIELD_DELIM);
            }

            String rowVal = sb.toString();
            ByteArrayInputStream bi = new ByteArrayInputStream(rowVal.getBytes(StandardCharsets.UTF_8));
            copyManager.copyIn(copySql, bi);
        } catch (Exception e) {
            processWriteException(e, index, row);
        }
    }

    private void writeSingleRecordCommit(Row row) throws WriteRecordException {
        try {
            super.writeSingleRecordInternal(row);
            try {
                dbConn.commit();
            } catch (Exception e) {
                // 提交失败直接结束任务
                throw new RuntimeException(e);
            }
        } catch (WriteRecordException e) {
            try {
                dbConn.rollback();
            } catch (Exception e1) {
                // 回滚失败直接结束任务
                throw new RuntimeException(e);
            }

            throw e;
        }
    }

    @Override
    protected void writeMultipleRecordsInternal() throws Exception {
        if(!checkIsCopyMode(insertSqlMode)){
            writeMultipleRecordsCommit();
            return;
        }

        StringBuilder sb = new StringBuilder(128);
        for (Row row : rows) {
            int lastIndex = row.getArity() - 1;
            for (int index =0; index < row.getArity(); index++) {
                Object rowData = getField(row, index);
                sb.append(rowData);
                if(index != lastIndex){
                    sb.append(DEFAULT_FIELD_DELIM);
                }
            }

            sb.append(LINE_DELIMITER);
        }

        String rowVal = sb.toString();
        ByteArrayInputStream bi = new ByteArrayInputStream(rowVal.getBytes(StandardCharsets.UTF_8));
        copyManager.copyIn(copySql, bi);

        if(restoreConfig.isRestore()){
            rowsOfCurrentTransaction += rows.size();
        }
    }

    private void writeMultipleRecordsCommit() throws Exception {
        try {
            super.writeMultipleRecordsInternal();
            dbConn.commit();
        } catch (Exception e){
            dbConn.rollback();
            throw e;
        }
    }

    @Override
    protected Object getField(Row row, int index) {
        Object field = super.getField(row, index);
        String type = columnType.get(index);
        field = typeConverter.convert(field,type);

        return field;
    }

    private boolean checkIsCopyMode(String insertMode){
        if(insertMode == null || insertMode.length() == 0){
            return false;
        }

        if(!INSERT_SQL_MODE_TYPE.equalsIgnoreCase(insertMode)){
            throw new RuntimeException("not support insertSqlMode:" + insertMode);
        }

        return true;
    }


}
