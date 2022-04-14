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
package com.dtstack.flinkx.connector.jdbc.sink;

import com.dtstack.flinkx.conf.FieldConf;
import com.dtstack.flinkx.connector.jdbc.conf.JdbcConf;
import com.dtstack.flinkx.connector.jdbc.dialect.JdbcDialect;
import com.dtstack.flinkx.connector.jdbc.statement.FieldNamedPreparedStatement;
import com.dtstack.flinkx.constants.CDCConstantValue;
import com.dtstack.flinkx.converter.AbstractRowConverter;
import com.dtstack.flinkx.element.ColumnRowData;

import com.dtstack.flinkx.element.column.BigDecimalColumn;
import com.dtstack.flinkx.element.column.BooleanColumn;
import com.dtstack.flinkx.element.column.NullColumn;
import com.dtstack.flinkx.element.column.StringColumn;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.types.RowKind;

import com.esotericsoftware.minlog.Log;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.commons.collections.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * build prepare proxy, proxy implements FieldNamedPreparedStatement. it support to build
 * preparestmt and manager it with cache.
 *
 * <p></>Company: www.dtstack.com
 *
 * @author xuchao
 * @date 2021-12-20
 */
public class PreparedStmtProxy implements FieldNamedPreparedStatement {

    private static final Logger LOG = LoggerFactory.getLogger(PreparedStmtProxy.class);

    private final int cacheSize = 100;

    private final int cacheDurationMin = 10;

    /** LUR cache key info: database_table_rowkind * */
    protected Cache<String, DynamicPreparedStmt> pstmtCache;

    /** 当前的执行sql的preparestatement */
    protected transient FieldNamedPreparedStatement currentFieldNamedPstmt;

    /** 当前执行sql的数据类型转换器 */
    protected AbstractRowConverter currentRowConverter;

    /** 当调用writeMultipleRecords 可能会涉及到多个pstmt */
    private final Set<FieldNamedPreparedStatement> unExecutePstmt = new LinkedHashSet<>();

    protected Connection connection;
    protected JdbcDialect jdbcDialect;
    protected JdbcConf jdbcConf;

    /** 是否将框架额外添加的扩展信息写入到数据库,默认不写入* */
    protected boolean writeExtInfo;

    public PreparedStmtProxy(Connection connection, JdbcDialect jdbcDialect, boolean writeExtInfo) {
        this.connection = connection;
        this.jdbcDialect = jdbcDialect;
        this.writeExtInfo = writeExtInfo;
        initCache(true);
    }

    public PreparedStmtProxy(
            FieldNamedPreparedStatement currentFieldNamedPstmt,
            AbstractRowConverter currentRowConverter,
            Connection connection,
            JdbcConf jdbcConf,
            JdbcDialect jdbcDialect) {
        this.currentFieldNamedPstmt = currentFieldNamedPstmt;
        this.currentRowConverter = currentRowConverter;
        this.connection = connection;
        this.jdbcConf = jdbcConf;
        this.jdbcDialect = jdbcDialect;
        initCache(false);
        this.pstmtCache.put(
                getPstmtCacheKey(jdbcConf.getSchema(), jdbcConf.getTable(), RowKind.INSERT),
                DynamicPreparedStmt.buildStmt(
                        jdbcConf,
                        jdbcDialect,
                        jdbcConf.getColumn(),
                        currentRowConverter,
                        currentFieldNamedPstmt));
    }

    public void convertToExternal(RowData row) throws Exception {
        getOrCreateFieldNamedPstmt(row);
        RowKind rowKind = row.getRowKind();
        switch (rowKind) {
            case DELETE:
                ColumnRowData columnRowData = convertColumnRowData(row);
                currentFieldNamedPstmt =
                        (FieldNamedPreparedStatement)
                                currentRowConverter.toExternal(
                                        columnRowData, this.currentFieldNamedPstmt);
                break;
            default:
                currentFieldNamedPstmt =
                        (FieldNamedPreparedStatement)
                                currentRowConverter.toExternal(row, this.currentFieldNamedPstmt);
                break;
        }
    }

    private ColumnRowData convertColumnRowData(RowData row) {
        List<String> uniqueKeys = jdbcConf.getUniqueKey();
        List<FieldConf> fullColumn = jdbcConf.getColumn();
        HashMap<String, FieldConf> map = new HashMap<>();
        HashMap<Integer, String> indexType = new HashMap<>();
        HashMap<Integer, FieldConf> indexMap = new HashMap<>();
        fullColumn.forEach(
                fieldConf -> {
                    String name = fieldConf.getName();
                    Integer index = fieldConf.getIndex();
                    String type = fieldConf.getType();
                    map.put(name, fieldConf);
                    indexMap.put(index, fieldConf);
                    indexType.put(index, type);
                });
        Set<Integer> indexs = new HashSet<>();
        uniqueKeys.forEach(
                (uniqueKey) -> {
                    FieldConf fieldConf = map.getOrDefault(uniqueKey, null);
                    if (null != fieldConf) {
                        indexs.add(fieldConf.getIndex());
                    }
                });
        // TODO 初始化 ColumnRowData
        ColumnRowData columnRowData = new ColumnRowData(row.getRowKind(), uniqueKeys.size());
        if (row instanceof GenericRowData) {
            GenericRowData genericRowData = (GenericRowData) row;
            indexs.forEach(
                    (index) -> {
                        Object field = genericRowData.getField(index);
                        FieldConf fieldConf = indexMap.get(index);
                        String column = fieldConf.getName();
                        if (field instanceof BinaryStringData) {
                            BinaryStringData binaryStringData = (BinaryStringData) field;
                            columnRowData.addField(
                                    new StringColumn(binaryStringData.getJavaObject()));
                        } else if (field instanceof DecimalData) {
                            DecimalData decimalData = (DecimalData) field;
                            columnRowData.addField(
                                    new BigDecimalColumn(decimalData.toBigDecimal()));
                        } else if (field instanceof Integer) {
                            Integer integer = (Integer) field;
                            columnRowData.addField(new BigDecimalColumn(integer));
                        } else if (field instanceof Float) {
                            Float aFloat = (Float) field;
                            columnRowData.addField(new BigDecimalColumn(aFloat));
                        } else {
                            columnRowData.addField(new NullColumn());
                        }
                        columnRowData.addHeader(column);
                        columnRowData.addExtHeader(column);
                    });
        } else if (row instanceof BinaryRowData) {
            BinaryRowData binaryRowData = (BinaryRowData) row;
            indexs.forEach(
                    (index) -> {
                        FieldConf fieldConf = indexMap.get(index);
                        String column = fieldConf.getName();
                        String type = indexType.get(index);
                        String temp = null;
                        if (type.contains("NOT NULL")) {
                            temp = type.substring(0, type.indexOf("NOT NULL"));
                        } else {
                            temp = type;
                        }

                        switch (temp) {
                            case "FLOAT":
                                float aFloat = binaryRowData.getFloat(index);
                                columnRowData.addField(new BigDecimalColumn(aFloat));
                                break;
                            case "STRING":
                                StringData string = binaryRowData.getString(index);
                                columnRowData.addField(new StringColumn(string.toString()));
                                break;
                            case "INTERGER":
                                int rowDataInt = binaryRowData.getInt(index);
                                columnRowData.addField(new BigDecimalColumn(rowDataInt));
                                break;
                            case "DOUBLE":
                                double aDouble = binaryRowData.getDouble(index);
                                columnRowData.addField(new BigDecimalColumn(aDouble));
                            case "BOOLEAN":
                                boolean aBoolean = binaryRowData.getBoolean(index);
                                columnRowData.addField(new BooleanColumn(aBoolean));
                            case "LONG":
                                long aLong = binaryRowData.getLong(index);
                                columnRowData.addField(new BigDecimalColumn(aLong));
                            default:
                                if (temp.startsWith("DECIMAL")) {
                                    String[] bracketContent = getBracketContent(temp.trim());
                                    if (bracketContent.length > 0) {
                                        String str = bracketContent[0];
                                        String[] split = str.split(",");
                                        int precision = Integer.valueOf(split[0].trim());
                                        int scale = Integer.valueOf(split[1].trim());
                                        DecimalData decimal =
                                                binaryRowData.getDecimal(index, precision, scale);
                                        columnRowData.addField(
                                                new BigDecimalColumn(decimal.toBigDecimal()));
                                    }
                                } else {
                                    columnRowData.addField(new NullColumn());
                                }
                                break;
                        }

                        columnRowData.addHeader(column);
                        columnRowData.addExtHeader(column);
                    });
        }
        return columnRowData;
    }

    private static String[] getBracketContent(String content) {
        String[] arr = new String[0];
        Pattern p = Pattern.compile("(?<=\\()[^\\)]+");
        Matcher m = p.matcher(content);
        while (m.find()) {
            arr = Arrays.copyOf(arr, arr.length + 1);
            arr[arr.length - 1] = m.group();
        }
        return arr;
    }

    public void getOrCreateFieldNamedPstmt(RowData row) throws ExecutionException {
        RowKind rowKind = row.getRowKind();
        if (row instanceof ColumnRowData) {
            ColumnRowData columnRowData = (ColumnRowData) row;
            Map<String, Integer> head = columnRowData.getHeaderInfo();
            if (MapUtils.isEmpty(head)) {
                return;
            }
            int dataBaseIndex = head.get(CDCConstantValue.SCHEMA);
            int tableIndex = head.get(CDCConstantValue.TABLE);

            String database = row.getString(dataBaseIndex).toString();
            String tableName = row.getString(tableIndex).toString();
            String key = getPstmtCacheKey(database, tableName, rowKind);

            DynamicPreparedStmt fieldNamedPreparedStatement =
                    pstmtCache.get(
                            key,
                            () -> {
                                try {
                                    return DynamicPreparedStmt.buildStmt(
                                            jdbcConf,
                                            columnRowData.getHeaderInfo(),
                                            columnRowData.getExtHeader(),
                                            database,
                                            tableName,
                                            columnRowData.getRowKind(),
                                            connection,
                                            jdbcDialect,
                                            writeExtInfo);
                                } catch (SQLException e) {
                                    LOG.warn("", e);
                                    return null;
                                }
                            });

            currentFieldNamedPstmt = fieldNamedPreparedStatement.getFieldNamedPreparedStatement();
            currentRowConverter = fieldNamedPreparedStatement.getRowConverter();
            if (!writeExtInfo) {
                columnRowData.removeExtHeaderInfo();
            }
        } else {
            String key = null;
            switch (rowKind) {
                case DELETE:
                    key =
                            getPstmtCacheKey(
                                    jdbcConf.getSchema(), jdbcConf.getTable(), RowKind.DELETE);
                    break;
                default:
                    key =
                            getPstmtCacheKey(
                                    jdbcConf.getSchema(), jdbcConf.getTable(), RowKind.INSERT);
                    break;
            }
            DynamicPreparedStmt fieldNamedPreparedStatement =
                    pstmtCache.get(
                            key,
                            () -> {
                                try {
                                    return DynamicPreparedStmt.buildStmt(
                                            jdbcConf,
                                            jdbcConf.getSchema(),
                                            jdbcConf.getTable(),
                                            rowKind,
                                            connection,
                                            jdbcDialect,
                                            jdbcConf.getColumn(),
                                            currentRowConverter);
                                } catch (SQLException e) {
                                    LOG.warn("", e);
                                    return null;
                                }
                            });
            currentFieldNamedPstmt = fieldNamedPreparedStatement.getFieldNamedPreparedStatement();
        }
    }

    public void writeSingleRecordInternal(RowData row) throws Exception {
        getOrCreateFieldNamedPstmt(row);
        RowKind rowKind = row.getRowKind();
        switch (rowKind) {
            case DELETE:
                ColumnRowData columnRowData = convertColumnRowData(row);
                currentFieldNamedPstmt =
                        (FieldNamedPreparedStatement)
                                currentRowConverter.toExternal(
                                        columnRowData, this.currentFieldNamedPstmt);
                break;
            default:
                currentFieldNamedPstmt =
                        (FieldNamedPreparedStatement)
                                currentRowConverter.toExternal(row, this.currentFieldNamedPstmt);
                break;
        }
        currentFieldNamedPstmt.execute();
    }

    protected void initCache(boolean isExpired) {
        CacheBuilder<String, DynamicPreparedStmt> cacheBuilder =
                CacheBuilder.newBuilder()
                        .maximumSize(cacheSize)
                        .removalListener(
                                notification -> {
                                    try {
                                        assert notification.getValue() != null;
                                        notification.getValue().close();
                                    } catch (SQLException e) {
                                        Log.error("", e);
                                    }
                                });
        if (isExpired) {
            cacheBuilder.expireAfterAccess(cacheDurationMin, TimeUnit.MINUTES);
        }
        this.pstmtCache = cacheBuilder.build();
    }

    public String getPstmtCacheKey(String schema, String table, RowKind rowKind) {
        return String.format("%s_%s_%s", schema, table, rowKind);
    }

    @Override
    public void clearParameters() throws SQLException {
        currentFieldNamedPstmt.clearParameters();
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        return currentFieldNamedPstmt.executeQuery();
    }

    @Override
    public void addBatch() throws SQLException {
        currentFieldNamedPstmt.addBatch();
        unExecutePstmt.add(currentFieldNamedPstmt);
    }

    @Override
    public int[] executeBatch() throws SQLException {
        List<Integer> exeResult = new ArrayList<>();
        for (FieldNamedPreparedStatement pstmt : unExecutePstmt) {
            int[] resultArray = pstmt.executeBatch();
            Arrays.stream(resultArray).forEach(exeResult::add);
        }

        int[] result = new int[exeResult.size()];
        for (int i = 0; i < exeResult.size(); i++) {
            result[i] = exeResult.get(i);
        }
        return result;
    }

    @Override
    public void clearBatch() throws SQLException {
        for (FieldNamedPreparedStatement pstmt : unExecutePstmt) {
            pstmt.clearBatch();
        }
        unExecutePstmt.clear();
    }

    @Override
    public boolean execute() throws SQLException {
        return currentFieldNamedPstmt.execute();
    }

    @Override
    public void setNull(int fieldIndex, int sqlType) throws SQLException {
        currentFieldNamedPstmt.setNull(fieldIndex, sqlType);
    }

    @Override
    public void setBoolean(int fieldIndex, boolean x) throws SQLException {
        currentFieldNamedPstmt.setBoolean(fieldIndex, x);
    }

    @Override
    public void setByte(int fieldIndex, byte x) throws SQLException {
        currentFieldNamedPstmt.setByte(fieldIndex, x);
    }

    @Override
    public void setShort(int fieldIndex, short x) throws SQLException {
        currentFieldNamedPstmt.setShort(fieldIndex, x);
    }

    @Override
    public void setInt(int fieldIndex, int x) throws SQLException {
        currentFieldNamedPstmt.setInt(fieldIndex, x);
    }

    @Override
    public void setLong(int fieldIndex, long x) throws SQLException {
        currentFieldNamedPstmt.setLong(fieldIndex, x);
    }

    @Override
    public void setFloat(int fieldIndex, float x) throws SQLException {
        currentFieldNamedPstmt.setFloat(fieldIndex, x);
    }

    @Override
    public void setDouble(int fieldIndex, double x) throws SQLException {
        currentFieldNamedPstmt.setDouble(fieldIndex, x);
    }

    @Override
    public void setBigDecimal(int fieldIndex, BigDecimal x) throws SQLException {
        currentFieldNamedPstmt.setBigDecimal(fieldIndex, x);
    }

    @Override
    public void setString(int fieldIndex, String x) throws SQLException {
        currentFieldNamedPstmt.setString(fieldIndex, x);
    }

    @Override
    public void setBytes(int fieldIndex, byte[] x) throws SQLException {
        currentFieldNamedPstmt.setBytes(fieldIndex, x);
    }

    @Override
    public void setDate(int fieldIndex, Date x) throws SQLException {
        currentFieldNamedPstmt.setDate(fieldIndex, x);
    }

    @Override
    public void setTime(int fieldIndex, Time x) throws SQLException {
        currentFieldNamedPstmt.setTime(fieldIndex, x);
    }

    @Override
    public void setTimestamp(int fieldIndex, Timestamp x) throws SQLException {
        currentFieldNamedPstmt.setTimestamp(fieldIndex, x);
    }

    @Override
    public void setObject(int fieldIndex, Object x) throws SQLException {
        currentFieldNamedPstmt.setObject(fieldIndex, x);
    }

    @Override
    public void setBlob(int fieldIndex, InputStream is) throws SQLException {
        currentFieldNamedPstmt.setBlob(fieldIndex, is);
    }

    @Override
    public void setClob(int fieldIndex, Reader reader) throws SQLException {
        currentFieldNamedPstmt.setClob(fieldIndex, reader);
    }

    @Override
    public void close() throws SQLException {
        currentFieldNamedPstmt.close();
    }
}
