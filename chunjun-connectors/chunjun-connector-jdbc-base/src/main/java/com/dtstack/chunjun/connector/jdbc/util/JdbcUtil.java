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

import com.dtstack.chunjun.conf.FieldConf;
import com.dtstack.chunjun.connector.jdbc.JdbcDialectWrapper;
import com.dtstack.chunjun.connector.jdbc.conf.JdbcConf;
import com.dtstack.chunjun.connector.jdbc.dialect.JdbcDialect;
import com.dtstack.chunjun.converter.RawTypeConverter;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;
import com.dtstack.chunjun.util.ClassUtil;
import com.dtstack.chunjun.util.ExceptionUtil;
import com.dtstack.chunjun.util.GsonUtil;
import com.dtstack.chunjun.util.RetryUtil;
import com.dtstack.chunjun.util.TableUtil;
import com.dtstack.chunjun.util.TelnetUtil;

import org.apache.flink.table.types.logical.LogicalType;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utilities for relational database connection and sql execution company: www.dtstack.com
 *
 * @author huyifan_zju@
 */
public class JdbcUtil {
    /** 增量任务过滤条件占位符 */
    public static final String INCREMENT_FILTER_PLACEHOLDER = "${incrementFilter}";
    /** 断点续传过滤条件占位符 */
    public static final String RESTORE_FILTER_PLACEHOLDER = "${restoreFilter}";

    public static final String TEMPORARY_TABLE_NAME = "chunjun_tmp";
    public static final String NULL_STRING = "null";
    private static final Logger LOG = LoggerFactory.getLogger(JdbcUtil.class);
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

    /**
     * 获取JDBC连接
     *
     * @param jdbcConf
     * @param jdbcDialect
     * @return
     */
    public static Connection getConnection(JdbcConf jdbcConf, JdbcDialect jdbcDialect) {
        TelnetUtil.telnet(jdbcConf.getJdbcUrl());
        ClassUtil.forName(
                jdbcDialect.defaultDriverName().get(),
                Thread.currentThread().getContextClassLoader());
        Properties prop = jdbcConf.getProperties();
        if (prop == null) {
            prop = new Properties();
        }
        if (org.apache.commons.lang3.StringUtils.isNotBlank(jdbcConf.getUsername())) {
            prop.put("user", jdbcConf.getUsername());
        }
        if (org.apache.commons.lang3.StringUtils.isNotBlank(jdbcConf.getPassword())) {
            prop.put("password", jdbcConf.getPassword());
        }
        Properties finalProp = prop;
        synchronized (ClassUtil.LOCK_STR) {
            return RetryUtil.executeWithRetry(
                    () -> DriverManager.getConnection(jdbcConf.getJdbcUrl(), finalProp),
                    3,
                    2000,
                    false);
        }
    }

    public static Connection getConnection(
            JdbcConf conf, org.apache.flink.connector.jdbc.dialect.JdbcDialect dialect) {
        return getConnection(conf, new JdbcDialectWrapper(dialect));
    }

    /**
     * get full column name and type from database
     *
     * @param cataLog cataLog
     * @param schema schema
     * @param tableName tableName
     * @param dbConn jdbc Connection
     * @return fullColumnList and fullColumnTypeList
     */
    public static Pair<List<String>, List<String>> getTableMetaData(
            String cataLog, String schema, String tableName, Connection dbConn) {
        try {
            // check table exists
            if (ALL_TABLE.equalsIgnoreCase(tableName.trim())) {
                return Pair.of(new LinkedList<>(), new LinkedList<>());
            }

            ResultSet tableRs = dbConn.getMetaData().getTables(cataLog, schema, tableName, null);
            if (!tableRs.next()) {
                String tableInfo = schema == null ? tableName : schema + "." + tableName;
                throw new ChunJunRuntimeException(String.format("table %s not found.", tableInfo));
            }

            ResultSet rs = dbConn.getMetaData().getColumns(cataLog, schema, tableName, null);
            List<String> fullColumnList = new LinkedList<>();
            List<String> fullColumnTypeList = new LinkedList<>();
            while (rs.next()) {
                // COLUMN_NAME
                fullColumnList.add(rs.getString(4));
                // TYPE_NAME
                fullColumnTypeList.add(rs.getString(6));
            }
            rs.close();
            return Pair.of(fullColumnList, fullColumnTypeList);
        } catch (SQLException e) {
            throw new ChunJunRuntimeException(
                    String.format("error to get meta from [%s.%s]", schema, tableName), e);
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

    /**
     * get primarykey
     *
     * @param schema
     * @param tableName
     * @param dbConn
     * @return
     * @throws SQLException
     */
    public static List<String> getTablePrimaryKey(
            String schema, String tableName, Connection dbConn) throws SQLException {
        ResultSet rs = dbConn.getMetaData().getPrimaryKeys(null, schema, tableName);
        List<String> indexList = new LinkedList<>();
        while (rs.next()) {
            String index = rs.getString(4);
            if (StringUtils.isNotBlank(index)) indexList.add(index);
        }
        return indexList;
    }

    public static List<String> getTableUniqueIndex(
            String schema, String tableName, Connection dbConn) throws SQLException {
        List<String> tablePrimaryKey = getTablePrimaryKey(schema, tableName, dbConn);
        if (CollectionUtils.isNotEmpty(tablePrimaryKey)) {
            return tablePrimaryKey;
        }

        ResultSet rs = dbConn.getMetaData().getIndexInfo(null, schema, tableName, true, false);
        List<String> indexList = new LinkedList<>();
        while (rs.next()) {
            String index = rs.getString(9);
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
                LOG.warn("Close resultSet error: {}", ExceptionUtil.getErrorMessage(e));
            }
        }

        if (null != stmt) {
            try {
                stmt.close();
            } catch (SQLException e) {
                LOG.warn("Close statement error:{}", ExceptionUtil.getErrorMessage(e));
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
                LOG.warn("Close connection error:{}", ExceptionUtil.getErrorMessage(e));
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
            LOG.warn("commit error:{}", ExceptionUtil.getErrorMessage(e));
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
            LOG.warn("rollBack error:{}", ExceptionUtil.getErrorMessage(e));
        }
    }

    /**
     * 获取结果集的列类型信息
     *
     * @param resultSet 查询结果集
     * @return 字段类型list列表
     */
    public static List<String> analyzeColumnType(ResultSet resultSet, List<FieldConf> metaColumns) {
        List<String> columnTypeList = new ArrayList<>();

        try {
            ResultSetMetaData rd = resultSet.getMetaData();
            Map<String, String> nameTypeMap = new HashMap<>((rd.getColumnCount() << 2) / 3);
            for (int i = 0; i < rd.getColumnCount(); ++i) {
                nameTypeMap.put(rd.getColumnName(i + 1), rd.getColumnTypeName(i + 1));
            }

            for (FieldConf metaColumn : metaColumns) {
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
            LOG.error(message);
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
     * @param jdbcConf jdbc datasource configuration
     * @return
     */
    public static void putExtParam(JdbcConf jdbcConf) {
        Properties properties = jdbcConf.getProperties();
        if (properties == null) {
            properties = new Properties();
        }
        properties.putIfAbsent("useCursorFetch", "true");
        properties.putIfAbsent("rewriteBatchedStatements", "true");
        jdbcConf.setProperties(properties);
    }

    /**
     * Add additional parameters to jdbc properties，
     *
     * @param jdbcConf jdbc datasource configuration
     * @param extraProperties default customConfiguration
     * @return
     */
    public static void putExtParam(JdbcConf jdbcConf, Properties extraProperties) {
        Properties properties = jdbcConf.getProperties();
        if (properties == null) {
            properties = new Properties();
        }
        Properties finalProperties = properties;
        extraProperties.forEach(finalProperties::putIfAbsent);

        jdbcConf.setProperties(finalProperties);
    }

    /**
     * 获取数据库的LogicalType
     *
     * @param jdbcConf 连接信息
     * @param jdbcDialect 方言
     * @param converter 数据库数据类型到flink内部类型的映射
     * @return
     */
    public static LogicalType getLogicalTypeFromJdbcMetaData(
            JdbcConf jdbcConf, JdbcDialect jdbcDialect, RawTypeConverter converter) {
        try (Connection conn = JdbcUtil.getConnection(jdbcConf, jdbcDialect)) {
            Pair<List<String>, List<String>> pair =
                    JdbcUtil.getTableMetaData(
                            null, jdbcConf.getSchema(), jdbcConf.getTable(), conn);
            List<String> rawFieldNames = pair.getLeft();
            List<String> rawFieldTypes = pair.getRight();
            return TableUtil.createRowType(rawFieldNames, rawFieldTypes, converter);
        } catch (SQLException throwables) {
            throw new RuntimeException(throwables);
        }
    }

    /** 解析schema.table 或者 "schema"."table"等格式的表名 获取对应的schema以及table * */
    public static void resetSchemaAndTable(JdbcConf jdbcConf, String leftQuote, String rightQuote) {
        String pattern =
                String.format(
                        "(?i)(%s(?<schema>(.*))%s\\.%s(?<table>(.*))%s)",
                        leftQuote, rightQuote, leftQuote, rightQuote);
        Pattern p = Pattern.compile(pattern);
        Matcher matcher = p.matcher(jdbcConf.getTable());
        String schema = null;
        String table = null;
        if (matcher.find()) {
            schema = matcher.group("schema");
            table = matcher.group("table");
        } else {
            String[] split = jdbcConf.getTable().split("\\.");
            if (split.length == 2) {
                schema = split[0];
                table = split[1];
            }
        }

        if (StringUtils.isNotBlank(schema)) {
            LOG.info(
                    "before reset table info, schema: {}, table: {}",
                    jdbcConf.getSchema(),
                    jdbcConf.getTable());

            jdbcConf.setSchema(schema);
            jdbcConf.setTable(table);
            LOG.info(
                    "after reset table info,schema: {},table: {}",
                    jdbcConf.getSchema(),
                    jdbcConf.getTable());
        }
    }
}
