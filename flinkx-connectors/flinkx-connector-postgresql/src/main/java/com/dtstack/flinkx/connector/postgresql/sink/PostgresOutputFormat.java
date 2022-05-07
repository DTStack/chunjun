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

package com.dtstack.flinkx.connector.postgresql.sink;

import com.dtstack.flinkx.connector.jdbc.converter.JdbcColumnConverter;
import com.dtstack.flinkx.connector.jdbc.sink.JdbcOutputFormat;
import com.dtstack.flinkx.connector.postgresql.dialect.PostgresqlDialect;
import com.dtstack.flinkx.constants.ConstantValue;
import com.dtstack.flinkx.element.ColumnRowData;
import com.dtstack.flinkx.enums.EWriteMode;
import com.dtstack.flinkx.throwable.NoRestartException;
import com.dtstack.flinkx.throwable.WriteRecordException;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.StringUtils;

import org.apache.commons.lang3.math.NumberUtils;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import java.io.ByteArrayInputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * @program: flinkx
 * @author: wuren
 * @create: 2021/08/12
 */
public class PostgresOutputFormat extends JdbcOutputFormat {

    // pg 字符串里含有\u0000 会报错 ERROR: invalid byte sequence for encoding "UTF8": 0x00
    public static final String SPACE = "\u0000";

    private static final String LINE_DELIMITER = "\n";
    private CopyManager copyManager;
    private boolean enableCopyMode = false;
    private String copySql = "";
    private static final String INSERT_SQL_MODE_TYPE = "copy";
    private static final String DEFAULT_FIELD_DELIMITER = "\001";

    private static final String DEFAULT_NULL_VALUE = "\002";

    /** 数据源类型信息 * */
    private final String dbType = DbType.POSTGRESQL.name();

    @Override
    protected void openInternal(int taskNumber, int numTasks) {
        super.openInternal(taskNumber, numTasks);
        try {
            // check is use copy mode for insert
            enableCopyMode = INSERT_SQL_MODE_TYPE.equalsIgnoreCase(jdbcConf.getInsertSqlMode());
            if (EWriteMode.INSERT.name().equalsIgnoreCase(jdbcConf.getMode()) && enableCopyMode) {
                copyManager = new CopyManager((BaseConnection) dbConn);

                PostgresqlDialect pgDialect = (PostgresqlDialect) jdbcDialect;
                copySql =
                        pgDialect.getCopyStatement(
                                jdbcConf.getTable(),
                                columnNameList.toArray(new String[0]),
                                StringUtils.isNullOrWhitespaceOnly(jdbcConf.getFieldDelim().trim())
                                        ? DEFAULT_FIELD_DELIMITER
                                        : jdbcConf.getFieldDelim(),
                                StringUtils.isNullOrWhitespaceOnly(jdbcConf.getNullDelim().trim())
                                        ? DEFAULT_NULL_VALUE
                                        : jdbcConf.getNullDelim());

                LOG.info("write sql:{}", copySql);
            }
            checkUpsert();
        } catch (SQLException sqe) {
            throw new IllegalArgumentException("checkUpsert() failed.", sqe);
        }
    }

    @Override
    protected void writeSingleRecordInternal(RowData row) throws WriteRecordException {
        if (!enableCopyMode) {
            super.writeSingleRecordInternal(row);
        } else {
            if (rowConverter instanceof JdbcColumnConverter) {
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
            if (rowConverter instanceof JdbcColumnConverter) {
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
                    StringUtils.isNullOrWhitespaceOnly(jdbcConf.getNullDelim().trim())
                            ? DEFAULT_NULL_VALUE
                            : jdbcConf.getNullDelim());

        } else {
            rowStr.append(col);
        }
        if (!isLast) {
            rowStr.append(
                    StringUtils.isNullOrWhitespaceOnly(jdbcConf.getFieldDelim().trim())
                            ? DEFAULT_FIELD_DELIMITER
                            : jdbcConf.getFieldDelim());
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

    /** 数据源类型 * */
    public enum DbType {
        POSTGRESQL,
        ADB
    }

    /**
     * 当mode为update时进行校验
     *
     * @return
     * @throws SQLException
     */
    public void checkUpsert() throws SQLException {
        if (EWriteMode.UPDATE.name().equalsIgnoreCase(jdbcConf.getMode())) {
            try (Connection connection = getConnection()) {

                // 效验版本
                String databaseProductVersion =
                        connection.getMetaData().getDatabaseProductVersion();
                LOG.info("source version is {}", databaseProductVersion);
                String[] split = databaseProductVersion.split("\\.");
                // 10.1.12
                if (split.length > 2) {
                    databaseProductVersion = split[0] + ConstantValue.POINT_SYMBOL + split[1];
                }

                if (NumberUtils.isNumber(databaseProductVersion)) {
                    BigDecimal sourceVersion = new BigDecimal(databaseProductVersion);
                    if (dbType.equalsIgnoreCase(DbType.POSTGRESQL.name())) {
                        // pg大于等于9.5
                        if (sourceVersion.compareTo(new BigDecimal("9.5")) < 0) {
                            throw new RuntimeException(
                                    "the postgreSql version is ["
                                            + databaseProductVersion
                                            + "] and must greater than or equal to 9.5 when you use update mode and source is "
                                            + DbType.POSTGRESQL.name());
                        }
                    } else if (dbType.equalsIgnoreCase(DbType.ADB.name())) {
                        // adb大于等于9.4
                        if (sourceVersion.compareTo(new BigDecimal("9.4")) < 0) {
                            throw new RuntimeException(
                                    "the postgreSql version is ["
                                            + databaseProductVersion
                                            + "] and must greater than or equal to 9.4 when you use update mode and source is "
                                            + DbType.ADB.name());
                        }
                    }
                }
            }
        }
    }
}
