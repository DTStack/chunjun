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

import com.dtstack.flinkx.constants.ConstantValue;
import com.dtstack.flinkx.enums.EWriteMode;
import com.dtstack.flinkx.exception.WriteRecordException;
import com.dtstack.flinkx.rdb.outputformat.JdbcOutputFormat;
import com.dtstack.flinkx.util.GsonUtil;
import com.dtstack.flinkx.util.RetryUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.flink.types.Row;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import java.io.ByteArrayInputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * when  postgresql with mode insert, it use 'copy tableName(columnName) from stdin' syntax
 * Date: 2019/8/5
 * Company: www.dtstack.com
 * @author xuchao
 */

public class PostgresqlOutputFormat extends JdbcOutputFormat {

    private static final String COPY_SQL_TEMPL = "copy %s(%s) from stdin DELIMITER '%s' NULL as '%s'";

    private static final String DEFAULT_FIELD_DELIM = "\001";

    private static final String DEFAULT_NULL_DELIM = "\002";

    private static final String LINE_DELIMITER = "\n";

    private boolean isCopyMode = false;

    /**
     * now just add ext insert mode:copy
     */
    private static final String INSERT_SQL_MODE_TYPE = "copy";

    private String copySql = "";

    private CopyManager copyManager;

    /** 数据源类型信息 **/
    private String sourceType = SourceType.POSTGRESQL.name();

    /** 根据表名查看对应的uniqueKey id **/
    protected static String GET_UNIQUE_KEY_ID_SQL = "select " +
            "F.relname,E.relid,C.indkey " +
            "from " +
            " PG_CLASS F " +
            "    left join PG_STAT_ALL_INDEXES E on F.OID = E.INDEXRELID " +
            "    left join PG_INDEX C on E.INDEXRELID = C.INDEXRELID " +
            "where %s E.RELNAME = ? and  indisunique = true order by indisprimary desc ";


    /** 根据字段Id以及表id 找到对应的字段名 **/
    protected final static String GET_UNIQUE_KEY_NAME_SQL = "select attname from pg_catalog.pg_attribute b where attnum in %s and attrelid = %s";


    @Override
    protected PreparedStatement prepareTemplates() throws SQLException {
        if(fullColumn == null || fullColumn.size() == 0) {
            fullColumn = column;
        }

        //check is use copy mode for insert
        isCopyMode = checkIsCopyMode(insertSqlMode);
        if (EWriteMode.INSERT.name().equalsIgnoreCase(mode) && isCopyMode) {
            copyManager = new CopyManager((BaseConnection) dbConn);
            copySql = String.format(COPY_SQL_TEMPL, table, String.join(",", column), DEFAULT_FIELD_DELIM, DEFAULT_NULL_DELIM);
            return null;
        }

        checkUpsert();
        return super.prepareTemplates();
    }

    @Override
    protected void writeSingleRecordInternal(Row row) throws WriteRecordException {
        if(!isCopyMode){
            super.writeSingleRecordInternal(row);
            return;
        }

        //write with copy
        int index = 0;
        try {
            StringBuilder sb = new StringBuilder();
            int lastIndex = row.getArity() - 1;
            for (; index < row.getArity(); index++) {
                Object rowData = getField(row, index);
                if(rowData==null){
                    sb.append(DEFAULT_NULL_DELIM);
                }else{
                    sb.append(rowData);
                }
                if(index != lastIndex){
                    sb.append(DEFAULT_FIELD_DELIM);
                }
            }
            String rowVal = sb.toString();
            if(rowVal.contains("\\")){
                rowVal=  rowVal.replaceAll("\\\\","\\\\\\\\");
            }
            if(rowVal.contains("\r")){
                rowVal=  rowVal.replaceAll("\r","\\\\r");
            }

            if(rowVal.contains("\n")){
                rowVal=  rowVal.replaceAll("\n","\\\\n");
            }
            ByteArrayInputStream bi = new ByteArrayInputStream(rowVal.getBytes(StandardCharsets.UTF_8));
            copyManager.copyIn(copySql, bi);
        } catch (Exception e) {
            processWriteException(e, index, row);
        }
    }

    @Override
    protected void writeMultipleRecordsInternal() throws Exception {
        if(!isCopyMode){
            super.writeMultipleRecordsInternal();
            return;
        }

        StringBuilder sb = new StringBuilder(128);
        for (Row row : rows) {
            int lastIndex = row.getArity() - 1;
            StringBuilder tempBuilder = new StringBuilder(128);
            for (int index =0; index < row.getArity(); index++) {
                Object rowData = getField(row, index);
                if(rowData==null){
                    tempBuilder.append(DEFAULT_NULL_DELIM);
                }else{
                    tempBuilder.append(rowData);
                }
                if(index != lastIndex){
                    tempBuilder.append(DEFAULT_FIELD_DELIM);
                }
            }
            // \r \n \ 等特殊字符串需要转义
            String tempData = tempBuilder.toString();
            if(tempData.contains("\\")){
                tempData=  tempData.replaceAll("\\\\","\\\\\\\\");
            }
            if(tempData.contains("\r")){
                tempData=  tempData.replaceAll("\r","\\\\r");
            }

            if(tempData.contains("\n")){
                tempData=  tempData.replaceAll("\n","\\\\n");
            }
            sb.append(tempData).append(LINE_DELIMITER);
        }

        String rowVal = sb.toString();
        ByteArrayInputStream bi = new ByteArrayInputStream(rowVal.getBytes(StandardCharsets.UTF_8));
        copyManager.copyIn(copySql, bi);

        if(restoreConfig.isRestore()){
            rowsOfCurrentTransaction += rows.size();
        }
    }

    @Override
    protected Object getField(Row row, int index) {
        Object field = super.getField(row, index);
        String type = columnType.get(index);
        field = typeConverter.convert(field,type);

        return field;
    }

    /**
     * 判断是否为copy模式
     * @param insertMode
     * @return
     */
    private boolean checkIsCopyMode(String insertMode){
        if(StringUtils.isBlank(insertMode)){
            return false;
        }

        if(!INSERT_SQL_MODE_TYPE.equalsIgnoreCase(insertMode)){
            throw new RuntimeException("not support insertSqlMode:" + insertMode);
        }

        return true;
    }


    /**
     * 获取uniqueKey
     * @param table
     * @param dbConn
     * @return
     * @throws SQLException
     */
    @Override
    protected Map<String, List<String>> probePrimaryKeys(String table, Connection dbConn) throws SQLException {
        Map<String, List<String>> map = new HashMap<>(16);

        if(StringUtils.isNotBlank(schema)){
            GET_UNIQUE_KEY_ID_SQL = String.format(GET_UNIQUE_KEY_ID_SQL,  "E.SCHEMANAME = ? and");
        }else{
            GET_UNIQUE_KEY_ID_SQL = String.format(GET_UNIQUE_KEY_ID_SQL,  "");
        }

        try (PreparedStatement ps = dbConn.prepareStatement(GET_UNIQUE_KEY_ID_SQL)) {
            String tableName = getTableName();
            if (StringUtils.isNotBlank(schema)) {
                ps.setString(1, schema);
                ps.setString(2, tableName);
            } else {
                ps.setString(1, tableName);
            }
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    String tableId = rs.getString("relid");
                    String columnName = rs.getString("indkey");
                    String uniqueName = rs.getString("relname");

                    try (PreparedStatement conflictPs = dbConn.prepareStatement(String.format(GET_UNIQUE_KEY_NAME_SQL, "(" +String.join(",", columnName.split(" ") )+ ")", tableId));
                         ResultSet conflictRs = conflictPs.executeQuery()) {
                        ArrayList<String> attNameList = new ArrayList<>(32);
                        while (conflictRs.next()) {
                            attNameList.add(conflictRs.getString("attname"));
                        }
                        if (CollectionUtils.isNotEmpty(attNameList)) {
                            map.put(uniqueName, attNameList);
                        }
                    }
                }
            }

            LOG.info("find conflict key is {}", GsonUtil.GSON.toJson(map));
            return map;
        }
    }

    public String getSourceType() {
        return sourceType;
    }

    public void setSourceType(String sourceType) {
        this.sourceType = sourceType;
    }

    /** 数据源类型 **/
    public enum SourceType {
        POSTGRESQL, ADB
    }

    /**
     * 当mode为update时进行校验
     *
     * @return
     * @throws SQLException
     */
    public void checkUpsert() throws SQLException {
        if (EWriteMode.UPDATE.name().equalsIgnoreCase(mode)) {
            try (Connection connection = RetryUtil.executeWithRetry(
                    () -> DriverManager.getConnection(
                            dbUrl,
                            username,
                            password),
                    3,
                    2000,
                    false)) {

                //效验版本
                String databaseProductVersion = connection.getMetaData().getDatabaseProductVersion();
                LOG.info("source version is {}", databaseProductVersion);
                String[] split = databaseProductVersion.split("\\.");
                //10.1.12
                if(split.length > 2){
                    databaseProductVersion =  split[0]+ ConstantValue.POINT_SYMBOL+split[1];
                }

                if (NumberUtils.isNumber(databaseProductVersion)) {
                    BigDecimal sourceVersion = new BigDecimal(databaseProductVersion);
                    if (sourceType.equalsIgnoreCase(PostgresqlOutputFormat.SourceType.POSTGRESQL.name())) {
                        //pg大于等于9.5
                        if (sourceVersion.compareTo(new BigDecimal("9.5")) < 0) {
                            throw new RuntimeException("the postgreSql version is [" + databaseProductVersion + "] and must greater than or equal to 9.5 when you use update mode and source is " + PostgresqlOutputFormat.SourceType.POSTGRESQL.name());
                        }
                    } else if (sourceType.equalsIgnoreCase(PostgresqlOutputFormat.SourceType.ADB.name())) {
                        //adb大于等于9.4
                        if (sourceVersion.compareTo(new BigDecimal("9.4")) < 0) {
                            throw new RuntimeException("the postgreSql version is [" + databaseProductVersion + "] and must greater than or equal to 9.4 when you use update mode and source is " + PostgresqlOutputFormat.SourceType.ADB.name());
                        }
                    }
                }
            }
        }
    }

    /**
     * 获取表名 如果是schema.table返回table
     * @return
     */
    private String getTableName() {
        //如果schema不为空 则table名为schema.table格式
        if (StringUtils.isNotBlank(schema)) {
            return table.substring(schema.length() + 1);
        }
        return table;
    }
}
