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

package com.dtstack.flinkx.kingbase.format;

import com.dtstack.flinkx.enums.EWriteMode;
import com.dtstack.flinkx.exception.WriteRecordException;
import com.dtstack.flinkx.rdb.outputformat.JdbcOutputFormat;
import com.dtstack.flinkx.util.ExceptionUtil;
import com.dtstack.flinkx.util.StringUtil;
import com.kingbase8.copy.CopyManager;
import com.kingbase8.core.BaseConnection;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.types.Row;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import static com.dtstack.flinkx.constants.ConstantValue.COMMA_SYMBOL;
import static com.dtstack.flinkx.kingbase.constants.KingbaseCons.DEFAULT_FIELD_DELIM;
import static com.dtstack.flinkx.kingbase.constants.KingbaseCons.DEFAULT_NULL_DELIM;
import static com.dtstack.flinkx.kingbase.constants.KingbaseCons.INSERT_SQL_MODE_TYPE;
import static com.dtstack.flinkx.kingbase.constants.KingbaseCons.LINE_DELIMITER;

/**
 * 写入数据到kingbase
 * Company: www.dtstack.com
 * @author kunni@dtstack.com
 */

public class KingbaseOutputFormat extends JdbcOutputFormat {

    private static final String COPY_SQL_TEMPL = "COPY %s(%s) FROM STDIN DELIMITER '%s' NULL AS '%s'";

    private String copySql = "";

    private CopyManager copyManager;


    @Override
    protected PreparedStatement prepareTemplates() throws SQLException {
        if(CollectionUtils.isEmpty(fullColumn)) {
            fullColumn = column;
        }

        //check is use copy mode for insert
        if (EWriteMode.INSERT.name().equalsIgnoreCase(mode) && checkIsCopyMode(insertSqlMode)) {
            copyManager = new CopyManager((BaseConnection) dbConn);
            copySql = String.format(COPY_SQL_TEMPL, table, String.join(COMMA_SYMBOL, column), DEFAULT_FIELD_DELIM, DEFAULT_NULL_DELIM);
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
            LOG.warn(ExceptionUtil.getErrorMessage(e));
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
            for (int index = 0; index < row.getArity(); index++) {
                Object rowData = getField(row, index);
                sb.append(rowData==null ? DEFAULT_NULL_DELIM : rowData);
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
        if(StringUtils.isEmpty(insertMode)){
            return false;
        }

        if(!INSERT_SQL_MODE_TYPE.equalsIgnoreCase(insertMode)){
            throw new RuntimeException("not support insertSqlMode:" + insertMode);
        }

        return true;
    }

}
