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

package com.dtstack.chunjun.connector.opengauss.sink;

import com.dtstack.chunjun.connector.jdbc.converter.JdbcSyncConverter;
import com.dtstack.chunjun.connector.jdbc.sink.JdbcOutputFormat;
import com.dtstack.chunjun.connector.opengauss.converter.OpengaussSyncConverter;
import com.dtstack.chunjun.connector.opengauss.dialect.OpengaussDialect;
import com.dtstack.chunjun.element.ColumnRowData;
import com.dtstack.chunjun.enums.EWriteMode;
import com.dtstack.chunjun.throwable.NoRestartException;
import com.dtstack.chunjun.throwable.WriteRecordException;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.StringUtils;

import lombok.extern.slf4j.Slf4j;
import org.opengauss.copy.CopyManager;
import org.opengauss.core.BaseConnection;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;

@Slf4j
public class OpengaussOutputFormat extends JdbcOutputFormat {

    private static final long serialVersionUID = 6244886558080900510L;

    // pg 字符串里含有\u0000 会报错 ERROR: invalid byte sequence for encoding "UTF8": 0x00
    public static final String SPACE = "\u0000";

    private static final String LINE_DELIMITER = "\n";
    private CopyManager copyManager;
    private boolean enableCopyMode = false;
    private String copySql = "";
    private static final String INSERT_SQL_MODE_TYPE = "copy";
    private static final String DEFAULT_FIELD_DELIMITER = "\001";

    private static final String DEFAULT_NULL_VALUE = "\002";

    @Override
    protected void openInternal(int taskNumber, int numTasks) {
        super.openInternal(taskNumber, numTasks);
        try {
            // check is use copy mode for insert
            enableCopyMode = INSERT_SQL_MODE_TYPE.equalsIgnoreCase(jdbcConfig.getInsertSqlMode());
            if (EWriteMode.INSERT.name().equalsIgnoreCase(jdbcConfig.getMode()) && enableCopyMode) {
                copyManager = new CopyManager((BaseConnection) dbConn);

                OpengaussDialect pgDialect = (OpengaussDialect) jdbcDialect;
                copySql =
                        pgDialect.getCopyStatement(
                                jdbcConfig.getSchema(),
                                jdbcConfig.getTable(),
                                columnNameList.toArray(new String[0]),
                                StringUtils.isNullOrWhitespaceOnly(
                                                jdbcConfig.getFieldDelim().trim())
                                        ? DEFAULT_FIELD_DELIMITER
                                        : jdbcConfig.getFieldDelim(),
                                StringUtils.isNullOrWhitespaceOnly(jdbcConfig.getNullDelim().trim())
                                        ? DEFAULT_NULL_VALUE
                                        : jdbcConfig.getNullDelim());

                log.info("write sql:{}", copySql);
            }
            if (rowConverter instanceof JdbcSyncConverter) {
                if (jdbcDialect.dialectName().equals("openGauss")) {
                    ((OpengaussSyncConverter) rowConverter).setConnection((BaseConnection) dbConn);
                }
            }
        } catch (SQLException sqe) {
            throw new IllegalArgumentException("checkUpsert() failed.", sqe);
        }
    }

    @Override
    protected void writeSingleRecordInternal(RowData row) throws WriteRecordException {
        if (!enableCopyMode) {
            super.writeSingleRecordInternal(row);
        } else {
            if (rowConverter instanceof JdbcSyncConverter) {
                ColumnRowData colRowData = (ColumnRowData) row;
                // write with copy
                int index = 0;
                try {
                    StringBuilder rowStr = new StringBuilder();
                    int lastIndex = row.getArity() - 1;
                    for (; index < row.getArity(); index++) {
                        appendColumn(colRowData, index, rowStr, index == lastIndex);
                    }
                    String rowVal = copyModeReplace(rowStr.toString());
                    ByteArrayInputStream bi =
                            new ByteArrayInputStream(rowVal.getBytes(StandardCharsets.UTF_8));
                    copyManager.copyIn(copySql, bi);
                } catch (Exception e) {
                    processWriteException(e, index, row);
                }
            } else {
                throw new NoRestartException("copy mode only support data sync with out table");
            }
        }
    }

    @Override
    protected void writeMultipleRecordsInternal() throws Exception {
        if (!enableCopyMode) {
            super.writeMultipleRecordsInternal();
        } else {
            if (rowConverter instanceof JdbcSyncConverter) {
                StringBuilder rowsStrBuilder = new StringBuilder(128);
                for (RowData row : rows) {
                    ColumnRowData colRowData = (ColumnRowData) row;
                    int lastIndex = row.getArity() - 1;
                    StringBuilder rowStr = new StringBuilder(128);
                    for (int index = 0; index < row.getArity(); index++) {
                        appendColumn(colRowData, index, rowStr, index == lastIndex);
                    }
                    String tempData = rowStr.toString();
                    rowsStrBuilder.append(copyModeReplace(tempData)).append(LINE_DELIMITER);
                }
                String rowVal = rowsStrBuilder.toString();
                ByteArrayInputStream bi =
                        new ByteArrayInputStream(rowVal.getBytes(StandardCharsets.UTF_8));
                copyManager.copyIn(copySql, bi);

                if (checkpointEnabled && CheckpointingMode.EXACTLY_ONCE == checkpointMode) {
                    rowsOfCurrentTransaction += rows.size();
                }
            } else {
                throw new NoRestartException("copy mode only support data sync with out table");
            }
        }
    }

    private void appendColumn(
            ColumnRowData colRowData, int pos, StringBuilder rowStr, boolean isLast) {
        Object col = colRowData.getField(pos);
        if (col == null) {
            rowStr.append(
                    StringUtils.isNullOrWhitespaceOnly(jdbcConfig.getNullDelim().trim())
                            ? DEFAULT_NULL_VALUE
                            : jdbcConfig.getNullDelim());

        } else {
            rowStr.append(col);
        }
        if (!isLast) {
            rowStr.append(
                    StringUtils.isNullOrWhitespaceOnly(jdbcConfig.getFieldDelim().trim())
                            ? DEFAULT_FIELD_DELIMITER
                            : jdbcConfig.getFieldDelim());
        }
    }

    /**
     * \r \n \ 等特殊字符串需要转义
     *
     * @return
     */
    private String copyModeReplace(String rowStr) {
        if (rowStr.contains("\\")) {
            rowStr = rowStr.replaceAll("\\\\", "\\\\\\\\");
        }
        if (rowStr.contains("\r")) {
            rowStr = rowStr.replaceAll("\r", "\\\\r");
        }

        if (rowStr.contains("\n")) {
            rowStr = rowStr.replaceAll("\n", "\\\\n");
        }

        // pg 字符串里含有\u0000 会报错 ERROR: invalid byte sequence for encoding "UTF8": 0x00
        if (rowStr.contains(SPACE)) {
            rowStr = rowStr.replaceAll(SPACE, "");
        }
        return rowStr;
    }
}
