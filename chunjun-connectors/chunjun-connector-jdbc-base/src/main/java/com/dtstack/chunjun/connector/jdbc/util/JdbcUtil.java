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
package com.dtstack.chunjun.connector.jdbc.util;

import com.dtstack.chunjun.config.FieldConfig;
import com.dtstack.chunjun.config.TypeConfig;
import com.dtstack.chunjun.connector.jdbc.conf.TableIdentify;
import com.dtstack.chunjun.connector.jdbc.config.JdbcConfig;
import com.dtstack.chunjun.connector.jdbc.dialect.JdbcDialect;
import com.dtstack.chunjun.connector.jdbc.source.JdbcInputSplit;
import com.dtstack.chunjun.constants.ConstantValue;
import com.dtstack.chunjun.element.ColumnRowData;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;
import com.dtstack.chunjun.util.ClassUtil;
import com.dtstack.chunjun.util.ExceptionUtil;
import com.dtstack.chunjun.util.GsonUtil;
import com.dtstack.chunjun.util.RetryUtil;
import com.dtstack.chunjun.util.TelnetUtil;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.DistinctType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.io.BufferedReader;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getFieldCount;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getPrecision;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getScale;

/** Utilities for relational database connection and sql execution */
@Slf4j
public class JdbcUtil {
    /** 增量任务过滤条件占位符 */
    public static final String INCREMENT_FILTER_PLACEHOLDER = "${incrementFilter}";
    /** 断点续传过滤条件占位符 */
    public static final String RESTORE_FILTER_PLACEHOLDER = "${restoreFilter}";

    public static final String TEMPORARY_TABLE_NAME = "chunjun_tmp";
    public static final String NULL_STRING = "null";
    /** 数据库连接的最大重试次数 */
    private static final int MAX_RETRY_TIMES = 3;
    /** 秒级时间戳的长度为10位 */
    private static final int SECOND_LENGTH = 10;
    /** 毫秒级时间戳的长度为13位 */
    private static final int MILLIS_LENGTH = 13;
    /** 微秒级时间戳的长度为16位 */
    private static final int MICRO_LENGTH = 16;
    /** 纳秒级时间戳的长度为19位 */
    private static final int NANOS_LENGTH = 19;

    private static final int FORMAT_TIME_NANOS_LENGTH = 29;
    private static final String ALL_TABLE = "*";
    public static int NANOS_PART_LENGTH = 9;

    public static Connection getConnection(JdbcConfig jdbcConfig, JdbcDialect jdbcDialect) {
        TelnetUtil.telnet(jdbcConfig.getJdbcUrl());
        ClassUtil.forName(
                jdbcDialect.defaultDriverName().orElseThrow(() -> new ChunJunRuntimeException("")),
                Thread.currentThread().getContextClassLoader());
        Properties prop = jdbcConfig.getProperties();
        if (prop == null) {
            prop = new Properties();
        }
        if (StringUtils.isNotBlank(jdbcConfig.getUsername())) {
            prop.put("user", jdbcConfig.getUsername());
        }
        if (StringUtils.isNotBlank(jdbcConfig.getPassword())) {
            prop.put("password", jdbcConfig.getPassword());
        }
        Properties finalProp = prop;
        synchronized (ClassUtil.LOCK_STR) {
            return RetryUtil.executeWithRetry(
                    () -> DriverManager.getConnection(jdbcConfig.getJdbcUrl(), finalProp),
                    3,
                    2000,
                    false);
        }
    }

    /** Check whether the table exists in the database */
    public static void checkTableExist(Connection dbConn, TableIdentify tableIdentify) {
        try {
            ResultSet tableRs =
                    dbConn.getMetaData()
                            .getTables(
                                    tableIdentify.getCatalog(),
                                    tableIdentify.getSchema(),
                                    tableIdentify.getTable(),
                                    null);
            if (!tableRs.next()) {
                throw new ChunJunRuntimeException(
                        String.format("table %s not found.", tableIdentify.getTableInfo()));
            }
        } catch (Throwable throwable) {
            throw new ChunJunRuntimeException("failed to check table exist", throwable);
        }
    }

    /**
     * get column name and type from database
     *
     * @param dbConn jdbc Connection.
     * @param metadataQuerySql SQL used to retrieve metadata
     */
    public static Pair<List<String>, List<TypeConfig>> getTableMetaData(
            Connection dbConn,
            String metadataQuerySql,
            int queryTimeout,
            Function<Tuple3<String, Integer, Integer>, TypeConfig> typeBuilder) {
        log.info("jdbc query table metadata from db, querySql is:{}", metadataQuerySql);
        try (Statement st = dbConn.createStatement();
                ResultSet rs = st.executeQuery(metadataQuerySql)) {
            st.setQueryTimeout(queryTimeout);
            ResultSetMetaData resultSetMetaData = rs.getMetaData();
            List<String> fullColumnList = new ArrayList<>(resultSetMetaData.getColumnCount());
            List<TypeConfig> fullColumnTypeList =
                    new ArrayList<>(resultSetMetaData.getColumnCount());
            String columnName;
            TypeConfig columnType;
            for (int i = 0; i < resultSetMetaData.getColumnCount(); i++) {
                columnName = resultSetMetaData.getColumnName(i + 1);
                columnType =
                        typeBuilder.apply(
                                Tuple3.of(
                                        resultSetMetaData.getColumnTypeName(i + 1),
                                        resultSetMetaData.getPrecision(i + 1),
                                        resultSetMetaData.getScale(i + 1)));
                fullColumnList.add(columnName);
                fullColumnTypeList.add(columnType);
            }
            rs.close();
            return Pair.of(fullColumnList, fullColumnTypeList);
        } catch (Exception e) {
            throw new ChunJunRuntimeException("failed to retrieve table metadata", e);
        }
    }

    /**
     * @param tableName
     * @param dbConn
     * @return
     * @throws SQLException
     */
    public static List<String> getTableIndex(String schema, String tableName, Connection dbConn)
            throws SQLException {
        ResultSet rs = dbConn.getMetaData().getIndexInfo(null, schema, tableName, true, false);
        List<String> indexList = new LinkedList<>();
        while (rs.next()) {
            String index = rs.getString(9);
            if (StringUtils.isNotBlank(index)) indexList.add(index);
        }
        return indexList;
    }

    public static List<String> getTableUniqueIndex(TableIdentify tableIdentify, Connection dbConn)
            throws SQLException {
        List<String> tablePrimaryKey = getTablePrimaryKey(tableIdentify, dbConn);
        if (CollectionUtils.isNotEmpty(tablePrimaryKey)) {
            return tablePrimaryKey;
        }

        ResultSet rs =
                dbConn.getMetaData()
                        .getIndexInfo(
                                tableIdentify.getCatalogForGetIndex(),
                                tableIdentify.getSchemaForGetIndex(),
                                tableIdentify.getTableForGetIndex(),
                                true,
                                false);
        List<String> indexList = new LinkedList<>();
        while (rs.next()) {
            String index = rs.getString(9);
            if (StringUtils.isNotBlank(index)) indexList.add(index);
        }
        return indexList;
    }

    /**
     * get primarykey from db
     *
     * @param dbConn
     * @return
     * @throws SQLException
     */
    public static List<String> getTablePrimaryKey(TableIdentify tableIdentify, Connection dbConn)
            throws SQLException {
        ResultSet rs =
                dbConn.getMetaData()
                        .getPrimaryKeys(
                                tableIdentify.getCatalog(),
                                tableIdentify.getSchema(),
                                tableIdentify.getTable());
        List<String> indexList = new LinkedList<>();
        while (rs.next()) {
            String index = rs.getString(4);
            if (StringUtils.isNotBlank(index)) indexList.add(index);
        }
        return indexList;
    }

    /**
     * 关闭连接资源
     *
     * @param rs ResultSet
     * @param stmt Statement
     * @param conn Connection
     * @param commit
     */
    public static void closeDbResources(
            ResultSet rs, Statement stmt, Connection conn, boolean commit) {
        if (null != rs) {
            try {
                rs.close();
            } catch (SQLException e) {
                log.warn("Close resultSet error: {}", ExceptionUtil.getErrorMessage(e));
            }
        }

        if (null != stmt) {
            try {
                stmt.close();
            } catch (SQLException e) {
                log.warn("Close statement error:{}", ExceptionUtil.getErrorMessage(e));
            }
        }

        if (null != conn) {
            try {
                if (commit) {
                    commit(conn);
                } else {
                    rollBack(conn);
                }

                conn.close();
            } catch (SQLException e) {
                log.warn("Close connection error:{}", ExceptionUtil.getErrorMessage(e));
            }
        }
    }

    /**
     * 手动提交事物
     *
     * @param conn Connection
     */
    public static void commit(Connection conn) {
        try {
            if (null != conn && !conn.isClosed() && !conn.getAutoCommit()) {
                conn.commit();
            }
        } catch (SQLException e) {
            log.warn("commit error:{}", ExceptionUtil.getErrorMessage(e));
        }
    }

    /**
     * 手动回滚事物
     *
     * @param conn Connection
     */
    public static void rollBack(Connection conn) {
        try {
            if (null != conn && !conn.isClosed() && !conn.getAutoCommit()) {
                conn.rollback();
            }
        } catch (SQLException e) {
            log.warn("rollBack error:{}", ExceptionUtil.getErrorMessage(e));
        }
    }

    /**
     * 获取结果集的列类型信息
     *
     * @param resultSet 查询结果集
     * @return 字段类型list列表
     */
    public static List<String> analyzeColumnType(
            ResultSet resultSet, List<FieldConfig> metaColumns) {
        List<String> columnTypeList = new ArrayList<>();

        try {
            ResultSetMetaData rd = resultSet.getMetaData();
            Map<String, String> nameTypeMap = new HashMap<>((rd.getColumnCount() << 2) / 3);
            for (int i = 0; i < rd.getColumnCount(); ++i) {
                nameTypeMap.put(rd.getColumnName(i + 1), rd.getColumnTypeName(i + 1));
            }

            for (FieldConfig metaColumn : metaColumns) {
                if (metaColumn.getValue() != null) {
                    columnTypeList.add("VARCHAR");
                } else {
                    columnTypeList.add(nameTypeMap.get(metaColumn.getName()));
                }
            }
        } catch (SQLException e) {
            String message =
                    String.format(
                            "error to analyzeSchema, resultSet = %s, columnTypeList = %s, e = %s",
                            resultSet,
                            GsonUtil.GSON.toJson(columnTypeList),
                            ExceptionUtil.getErrorMessage(e));
            log.error(message);
            throw new RuntimeException(message);
        }
        return columnTypeList;
    }

    /**
     * clob转string
     *
     * @param obj clob
     * @return
     * @throws Exception
     */
    public static Object clobToString(Object obj) throws Exception {
        String dataStr;
        if (obj instanceof Clob) {
            Clob clob = (Clob) obj;
            BufferedReader bf = new BufferedReader(clob.getCharacterStream());
            StringBuilder stringBuilder = new StringBuilder();
            String line;
            while ((line = bf.readLine()) != null) {
                stringBuilder.append(line);
            }
            dataStr = stringBuilder.toString();
        } else {
            return obj;
        }

        return dataStr;
    }

    /**
     * 获取纳秒字符串
     *
     * @param timeStr 2020-03-23 11:03:22.000000000
     * @return
     */
    public static String getNanosTimeStr(String timeStr) {
        if (timeStr.length() < FORMAT_TIME_NANOS_LENGTH) {
            timeStr += StringUtils.repeat("0", FORMAT_TIME_NANOS_LENGTH - timeStr.length());
        }
        return timeStr;
    }

    /**
     * 将边界位置时间转换成对应饿的纳秒时间
     *
     * @param startLocation 边界位置(起始/结束)
     * @return
     */
    public static int getNanos(long startLocation) {
        String timeStr = String.valueOf(startLocation);
        int nanos;
        if (timeStr.length() == SECOND_LENGTH) {
            nanos = 0;
        } else if (timeStr.length() == MILLIS_LENGTH) {
            nanos = Integer.parseInt(timeStr.substring(SECOND_LENGTH, MILLIS_LENGTH)) * 1000000;
        } else if (timeStr.length() == MICRO_LENGTH) {
            nanos = Integer.parseInt(timeStr.substring(SECOND_LENGTH, MICRO_LENGTH)) * 1000;
        } else if (timeStr.length() == NANOS_LENGTH) {
            nanos = Integer.parseInt(timeStr.substring(SECOND_LENGTH, NANOS_LENGTH));
        } else {
            throw new IllegalArgumentException("Unknown time unit:startLocation=" + startLocation);
        }

        return nanos;
    }

    /**
     * 将边界位置时间转换成对应饿的毫秒时间
     *
     * @param startLocation 边界位置(起始/结束)
     * @return
     */
    public static long getMillis(long startLocation) {
        String timeStr = String.valueOf(startLocation);
        long millisSecond;
        if (timeStr.length() == SECOND_LENGTH) {
            millisSecond = startLocation * 1000;
        } else if (timeStr.length() == MILLIS_LENGTH) {
            millisSecond = startLocation;
        } else if (timeStr.length() == MICRO_LENGTH) {
            millisSecond = startLocation / 1000;
        } else if (timeStr.length() == NANOS_LENGTH) {
            millisSecond = startLocation / 1000000;
        } else {
            throw new IllegalArgumentException("Unknown time unit:startLocation=" + startLocation);
        }

        return millisSecond;
    }

    /**
     * Add additional parameters to jdbc properties，for MySQL
     *
     * @param jdbcConfig jdbc datasource configuration
     * @return
     */
    public static void putExtParam(JdbcConfig jdbcConfig) {
        Properties properties = jdbcConfig.getProperties();
        if (properties == null) {
            properties = new Properties();
        }
        properties.putIfAbsent("useCursorFetch", "true");
        properties.putIfAbsent("rewriteBatchedStatements", "true");
        properties.put("tinyInt1isBit", "false");
        jdbcConfig.setProperties(properties);
    }

    /**
     * Add additional parameters to jdbc properties，
     *
     * @param jdbcConfig jdbc datasource configuration
     * @param extraProperties default customConfiguration
     * @return
     */
    public static void putExtParam(JdbcConfig jdbcConfig, Properties extraProperties) {
        Properties properties = jdbcConfig.getProperties();
        if (properties == null) {
            properties = new Properties();
        }
        Properties finalProperties = properties;
        extraProperties.forEach(finalProperties::putIfAbsent);

        jdbcConfig.setProperties(finalProperties);
    }

    /** 解析schema.table 或者 "schema"."table"等格式的表名 获取对应的schema以及table * */
    public static void resetSchemaAndTable(
            JdbcConfig jdbcConfig, String leftQuote, String rightQuote) {
        String pattern =
                String.format(
                        "(?i)(%s(?<schema>(.*))%s\\.%s(?<table>(.*))%s)",
                        leftQuote, rightQuote, leftQuote, rightQuote);
        Pattern p = Pattern.compile(pattern);
        Matcher matcher = p.matcher(jdbcConfig.getTable());
        String schema = null;
        String table = null;
        if (matcher.find()) {
            schema = matcher.group("schema");
            table = matcher.group("table");
        } else {
            String[] split = jdbcConfig.getTable().split("\\.");
            if (split.length == 2) {
                schema = split[0];
                table = split[1];
            }
        }

        if (StringUtils.isNotBlank(schema)) {
            log.info(
                    "before reset table info, schema: {}, table: {}",
                    jdbcConfig.getSchema(),
                    jdbcConfig.getTable());

            jdbcConfig.setSchema(schema);
            jdbcConfig.setTable(table);
            log.info(
                    "after reset table info,schema: {},table: {}",
                    jdbcConfig.getSchema(),
                    jdbcConfig.getTable());
        }
    }

    public static Pair<List<String>, List<TypeConfig>> buildCustomColumnInfo(
            List<FieldConfig> column, String constantType) {
        List<String> columnNameList = new ArrayList<>(column.size());
        List<TypeConfig> columnTypeList = new ArrayList<>(column.size());
        int index = 0;
        for (FieldConfig fieldConfig : column) {
            if (StringUtils.isNotBlank(fieldConfig.getValue())) {
                fieldConfig.setType(TypeConfig.fromString(constantType));
                fieldConfig.setIndex(-1);
            } else {
                columnNameList.add(fieldConfig.getName());
                columnTypeList.add(fieldConfig.getType());
                fieldConfig.setIndex(index++);
            }
        }
        return Pair.of(columnNameList, columnTypeList);
    }

    public static Pair<List<String>, List<TypeConfig>> buildColumnWithMeta(
            JdbcConfig jdbcConfig,
            Pair<List<String>, List<TypeConfig>> tableMetaData,
            String constantType) {
        List<String> metaColumnName = tableMetaData.getLeft();
        List<TypeConfig> metaColumnType = tableMetaData.getRight();

        List<FieldConfig> column = jdbcConfig.getColumn();
        int size = metaColumnName.size();
        List<String> columnNameList = new ArrayList<>(size);
        List<TypeConfig> columnTypeList = new ArrayList<>(size);
        if (column.size() == 1 && ConstantValue.STAR_SYMBOL.equals(column.get(0).getName())) {
            List<FieldConfig> metaColumn = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                FieldConfig fieldConfig = new FieldConfig();
                fieldConfig.setName(metaColumnName.get(i));
                columnNameList.add(metaColumnName.get(i));
                fieldConfig.setType(metaColumnType.get(i));
                columnTypeList.add(metaColumnType.get(i));
                fieldConfig.setIndex(i);
                metaColumn.add(fieldConfig);
            }
            jdbcConfig.setColumn(metaColumn);
            return Pair.of(columnNameList, columnTypeList);
        } else {
            return checkAndModifyColumnWithMeta(
                    jdbcConfig.getTable(),
                    jdbcConfig.getColumn(),
                    metaColumnName,
                    metaColumnType,
                    constantType);
        }
    }

    private static Pair<List<String>, List<TypeConfig>> checkAndModifyColumnWithMeta(
            String tableName,
            List<FieldConfig> column,
            List<String> metaColumnName,
            List<TypeConfig> metaColumnType,
            String constantType) {
        // check columnName and modify columnType
        int metaColumnSize = metaColumnName.size();
        List<String> columnNameList = new ArrayList<>(column.size());
        List<TypeConfig> columnTypeList = new ArrayList<>(column.size());
        int index = 0;
        for (FieldConfig fieldConfig : column) {
            if (StringUtils.isNotBlank(fieldConfig.getValue())) {
                fieldConfig.setType(TypeConfig.fromString(constantType));
                fieldConfig.setIndex(-1);
            } else {
                String name = fieldConfig.getName();
                TypeConfig metaType = null;
                int i = 0;
                for (; i < metaColumnSize; i++) {
                    // todo get precision and scale
                    if (metaColumnName.get(i).equalsIgnoreCase(name)) {
                        metaType = metaColumnType.get(i);
                        columnNameList.add(name);
                        columnTypeList.add(metaType);
                        fieldConfig.setIndex(index++);
                        fieldConfig.setType(metaColumnType.get(i));
                        break;
                    }
                }
                if (i == metaColumnSize) {
                    throw new ChunJunRuntimeException(
                            String.format(
                                    "The column[%s] does not exist in the table[%s]",
                                    name, tableName));
                }
                assert metaType != null
                        : String.format("failed to get column type from db,column name= %s ", name);
            }
        }
        return Pair.of(columnNameList, columnTypeList);
    }

    public static void setStarLocationForSplits(JdbcInputSplit[] splits, String startLocation) {
        if (StringUtils.isNotBlank(startLocation)) {
            String[] locations = startLocation.split(ConstantValue.COMMA_SYMBOL);
            if (locations.length != 1 && splits.length != locations.length) {
                throw new IllegalArgumentException(
                        "The number of startLocations is not equal to the number of channels");
            }
            if (locations.length == 1) {
                for (JdbcInputSplit split : splits) {
                    split.setStartLocation(locations[0]);
                }
            } else {
                for (int i = 0; i < splits.length; i++) {
                    splits[i].setStartLocation(locations[i]);
                }
            }
        }
    }

    public static Function<RowData, RowData> getKeyExtractor(
            List<String> columnList,
            List<String> keyList,
            RowType keyRowType,
            boolean useAbstractBaseColumn) {
        List<LogicalType> keyLogicalTypeList =
                keyRowType.getFields().stream()
                        .map(RowType.RowField::getType)
                        .collect(Collectors.toList());
        int[] keyIndices = new int[keyList.size()];
        for (int i = 0; i < keyList.size(); i++) {
            keyIndices[i] = columnList.indexOf(keyList.get(i));
        }
        if (useAbstractBaseColumn) {
            return row -> {
                ColumnRowData pkRow = new ColumnRowData(keyList.size());
                for (int i = 0; i < keyList.size(); i++) {
                    pkRow.addField(((ColumnRowData) row).getField(keyIndices[i]));
                }
                return pkRow;
            };
        } else {
            final RowData.FieldGetter[] fieldGetters = new RowData.FieldGetter[keyList.size()];
            for (int i = 0; i < keyList.size(); i++) {
                fieldGetters[i] = createFieldGetter(keyLogicalTypeList.get(i), keyIndices[i]);
            }
            return row -> {
                GenericRowData pkRow = new GenericRowData(fieldGetters.length);
                for (int i = 0; i < fieldGetters.length; i++) {
                    pkRow.setField(i, fieldGetters[i].getFieldOrNull(row));
                }
                return pkRow;
            };
        }
    }

    static RowData.FieldGetter createFieldGetter(LogicalType fieldType, int fieldPos) {
        final RowData.FieldGetter fieldGetter;
        // ordered by type root definition
        switch (fieldType.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                fieldGetter = row -> row.getString(fieldPos);
                break;
            case BOOLEAN:
                fieldGetter = row -> row.getBoolean(fieldPos);
                break;
            case BINARY:
            case VARBINARY:
                fieldGetter = row -> row.getBinary(fieldPos);
                break;
            case DECIMAL:
                final int decimalPrecision = getPrecision(fieldType);
                final int decimalScale = getScale(fieldType);
                fieldGetter = row -> row.getDecimal(fieldPos, decimalPrecision, decimalScale);
                break;
            case TINYINT:
                fieldGetter = row -> row.getByte(fieldPos);
                break;
            case SMALLINT:
                fieldGetter = row -> row.getShort(fieldPos);
                break;
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
            case INTERVAL_YEAR_MONTH:
                fieldGetter = row -> row.getInt(fieldPos);
                break;
            case BIGINT:
            case INTERVAL_DAY_TIME:
                fieldGetter = row -> row.getLong(fieldPos);
                break;
            case FLOAT:
                fieldGetter = row -> row.getFloat(fieldPos);
                break;
            case DOUBLE:
                fieldGetter = row -> row.getDouble(fieldPos);
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                final int timestampPrecision = getPrecision(fieldType);
                fieldGetter = row -> row.getTimestamp(fieldPos, timestampPrecision);
                break;
            case TIMESTAMP_WITH_TIME_ZONE:
                throw new UnsupportedOperationException();
            case ARRAY:
                fieldGetter = row -> row.getArray(fieldPos);
                break;
            case MULTISET:
            case MAP:
                fieldGetter = row -> row.getMap(fieldPos);
                break;
            case ROW:
            case STRUCTURED_TYPE:
                final int rowFieldCount = getFieldCount(fieldType);
                fieldGetter = row -> row.getRow(fieldPos, rowFieldCount);
                break;
            case DISTINCT_TYPE:
                fieldGetter =
                        createFieldGetter(((DistinctType) fieldType).getSourceType(), fieldPos);
                break;
            case RAW:
                fieldGetter = row -> row.getRawValue(fieldPos);
                break;
            case NULL:
            case SYMBOL:
            case UNRESOLVED:
            default:
                throw new IllegalArgumentException();
        }
        if (!fieldType.isNullable()) {
            return fieldGetter;
        }
        return row -> {
            if (row.isNullAt(fieldPos)) {
                return null;
            }
            return fieldGetter.getFieldOrNull(row);
        };
    }
}
