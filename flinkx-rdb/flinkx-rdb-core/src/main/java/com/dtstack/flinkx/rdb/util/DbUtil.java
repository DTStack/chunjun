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
package com.dtstack.flinkx.rdb.util;

import com.dtstack.flinkx.constants.ConstantValue;
import com.dtstack.flinkx.rdb.DatabaseInterface;
import com.dtstack.flinkx.rdb.ParameterValuesProvider;
import com.dtstack.flinkx.reader.MetaColumn;
import com.dtstack.flinkx.util.ClassUtil;
import com.dtstack.flinkx.util.ExceptionUtil;
import com.dtstack.flinkx.util.SysUtil;
import com.dtstack.flinkx.util.TelnetUtil;
import com.google.gson.Gson;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.util.CollectionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Utilities for relational database connection and sql execution
 * company: www.dtstack.com
 *
 * @author huyifan_zju@
 */
public class DbUtil {

    private static final Logger LOG = LoggerFactory.getLogger(DbUtil.class);

    /**
     * 数据库连接的最大重试次数
     */
    private static int MAX_RETRY_TIMES = 3;

    /**
     * 秒级时间戳的长度为10位
     */
    private static int SECOND_LENGTH = 10;
    /**
     * 毫秒级时间戳的长度为13位
     */
    private static int MILLIS_LENGTH = 13;
    /**
     * 微秒级时间戳的长度为16位
     */
    private static int MICRO_LENGTH = 16;
    /**
     * 纳秒级时间戳的长度为19位
     */
    private static int NANOS_LENGTH = 19;

    /**
     * jdbc连接URL的分割正则，用于获取URL?后的连接参数
     */
    public static final Pattern DB_PATTERN = Pattern.compile("\\?");

    /**
     * 增量任务过滤条件占位符
     */
    public static final String INCREMENT_FILTER_PLACEHOLDER = "${incrementFilter}";

    /**
     * 断点续传过滤条件占位符
     */
    public static final String RESTORE_FILTER_PLACEHOLDER = "${restoreFilter}";

    public static final String TEMPORARY_TABLE_NAME = "flinkx_tmp";

    public static final String NULL_STRING = "null";

    /**
     * 获取jdbc连接(超时10S)
     *
     * @param url      url
     * @param username 账号
     * @param password 密码
     * @return
     * @throws SQLException
     */
    private static Connection getConnectionInternal(String url, String username, String password) throws SQLException {
        Connection dbConn;
        synchronized (ClassUtil.LOCK_STR) {
            DriverManager.setLoginTimeout(10);

            // telnet
            TelnetUtil.telnet(url);

            if (username == null) {
                dbConn = DriverManager.getConnection(url);
            } else {
                dbConn = DriverManager.getConnection(url, username, password);
            }
        }

        return dbConn;
    }

    /**
     * 获取jdbc连接(重试3次)
     *
     * @param url      url
     * @param username 账号
     * @param password 密码
     * @return
     * @throws SQLException
     */
    public static Connection getConnection(String url, String username, String password) throws SQLException {
        if (!url.startsWith(ConstantValue.PROTOCOL_JDBC_MYSQL)) {
            return getConnectionInternal(url, username, password);
        } else {
            boolean failed = true;
            Connection dbConn = null;
            for (int i = 0; i < MAX_RETRY_TIMES && failed; ++i) {
                try {
                    dbConn = getConnectionInternal(url, username, password);
                    dbConn.createStatement().execute("select 111");
                    failed = false;
                } catch (Exception e) {
                    if (dbConn != null) {
                        dbConn.close();
                    }

                    if (i == MAX_RETRY_TIMES - 1) {
                        throw e;
                    } else {
                        SysUtil.sleep(3000);
                    }
                }
            }

            return dbConn;
        }
    }

    /**
     * 关闭连接资源
     *
     * @param rs     ResultSet
     * @param stmt   Statement
     * @param conn   Connection
     * @param commit
     */
    public static void closeDbResources(ResultSet rs, Statement stmt, Connection conn, boolean commit) {
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
            if (!conn.isClosed() && !conn.getAutoCommit()) {
                conn.commit();
            }
        } catch (SQLException e) {
            LOG.warn("commit error:{}", ExceptionUtil.getErrorMessage(e));
        }
    }

    /**
     * 批量执行sql
     *
     * @param dbConn Connection
     * @param sqls   sql列表
     */
    public static void executeBatch(Connection dbConn, List<String> sqls) {
        if (sqls == null || sqls.size() == 0) {
            return;
        }

        try {
            Statement stmt = dbConn.createStatement();
            for (String sql : sqls) {
                stmt.addBatch(sql);
            }
            stmt.executeBatch();
        } catch (SQLException e) {
            throw new RuntimeException("execute batch sql error:{}", e);
        } finally {
            commit(dbConn);
        }
    }

    /**
     * 获取某数据库某表的主键和唯一索引
     *
     * @param table  表名
     * @param dbConn 数据库连接
     * @return
     * @throws SQLException
     */
    public static Map<String, List<String>> getPrimaryOrUniqueKeys(String table, Connection dbConn) throws SQLException {
        Map<String, List<String>> keyMap = new HashMap<>(16);
        DatabaseMetaData meta = dbConn.getMetaData();
        ResultSet rs = meta.getIndexInfo(null, null, table, true, false);
        while (rs.next()) {
            String pkName = rs.getString(6);
            String columnName = rs.getString(9);
            if (!keyMap.containsKey(pkName)) {
                keyMap.put(pkName, new ArrayList<>());
            }
            keyMap.get(pkName).add(columnName);
        }
        return keyMap;
    }

    /**
     * 封装channel通道顺序
     *
     * @param channels
     * @return
     */
    public static Object[][] getParameterValues(final int channels) {
        ParameterValuesProvider provider = () -> {
            Integer[][] parameters = new Integer[channels][];
            for (int i = 0; i < channels; ++i) {
                parameters[i] = new Integer[2];
                parameters[i][0] = channels;
                parameters[i][1] = i;
            }
            return parameters;
        };

        return provider.getParameterValues();
    }

    /**
     * 获取表列名类型列表
     *
     * @param dbUrl             jdbc url
     * @param username          数据库账号
     * @param password          数据库密码
     * @param databaseInterface DatabaseInterface
     * @param table             表名
     * @param metaColumns    MetaColumn列表
     * @return 字段类型list列表
     */
    public static List<String> analyzeTable(String dbUrl, String username, String password, DatabaseInterface databaseInterface, String table, List<MetaColumn> metaColumns) {
        List<String> descColumnTypeList = new ArrayList<>(metaColumns.size());
        Connection dbConn = null;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            dbConn = getConnection(dbUrl, username, password);
            stmt = dbConn.createStatement();
            rs = stmt.executeQuery(databaseInterface.getSqlQueryFields(databaseInterface.quoteTable(table)));
            ResultSetMetaData rd = rs.getMetaData();

            boolean flag = ConstantValue.STAR_SYMBOL.equals(metaColumns.get(0).getName());
            Map<String, String> nameTypeMap = new HashMap<>((rd.getColumnCount() << 2) / 3);
            for (int i = 1; i <= rd.getColumnCount(); i++) {
                if(flag){
                    descColumnTypeList.add(rd.getColumnTypeName(i));
                }else{
                    nameTypeMap.put(rd.getColumnName(i), rd.getColumnTypeName(i));
                }
            }
            if(!flag){
                for (MetaColumn metaColumn : metaColumns) {
                    if (metaColumn.getValue() != null) {
                        descColumnTypeList.add("string");
                    } else {
                        descColumnTypeList.add(nameTypeMap.get(metaColumn.getName()));
                    }
                }
            }
        } catch (SQLException e) {
            LOG.error("error to analyzeTable, dbUrl =  {}, table = {}, metaColumns = {}, e = {}", dbUrl, table, new Gson().toJson(metaColumns), ExceptionUtil.getErrorMessage(e));
            throw new RuntimeException(e);
        } finally {
            closeDbResources(rs, stmt, dbConn, false);
        }

        return descColumnTypeList;
    }

    /**
     * 占位符设值
     *
     * @param param     参数
     * @param statement PreparedStatement
     * @param i         占位符位置
     * @throws SQLException
     */
    public static void setParameterValue(Object param, PreparedStatement statement, int i) throws SQLException {
        if (param instanceof String) {
            statement.setString(i + 1, (String) param);
        } else if (param instanceof Long) {
            statement.setLong(i + 1, (Long) param);
        } else if (param instanceof Integer) {
            statement.setInt(i + 1, (Integer) param);
        } else if (param instanceof Double) {
            statement.setDouble(i + 1, (Double) param);
        } else if (param instanceof Boolean) {
            statement.setBoolean(i + 1, (Boolean) param);
        } else if (param instanceof Float) {
            statement.setFloat(i + 1, (Float) param);
        } else if (param instanceof BigDecimal) {
            statement.setBigDecimal(i + 1, (BigDecimal) param);
        } else if (param instanceof Byte) {
            statement.setByte(i + 1, (Byte) param);
        } else if (param instanceof Short) {
            statement.setShort(i + 1, (Short) param);
        } else if (param instanceof Date) {
            statement.setDate(i + 1, (Date) param);
        } else if (param instanceof Time) {
            statement.setTime(i + 1, (Time) param);
        } else if (param instanceof Timestamp) {
            statement.setTimestamp(i + 1, (Timestamp) param);
        } else if (param instanceof Array) {
            statement.setArray(i + 1, (Array) param);
        } else {
            //extends with other types if needed
            throw new IllegalArgumentException("open() failed. Parameter " + i + " of type " + param.getClass() + " is not handled (yet).");
        }
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
     * @param timeStr
     * @return
     */
    public static String getNanosTimeStr(String timeStr) {
        if (timeStr.length() < 29) {
            timeStr += StringUtils.repeat("0", 29 - timeStr.length());
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
     * 格式化jdbc连接
     *
     * @param dbUrl       原jdbc连接
     * @param extParamMap 需要额外添加的参数
     * @return 格式化后jdbc连接URL字符串
     */
    public static String formatJdbcUrl(String dbUrl, Map<String, String> extParamMap) {
        String[] splits = DB_PATTERN.split(dbUrl);

        Map<String, String> paramMap = new HashMap<>(16);
        if (splits.length > 1) {
            String[] pairs = splits[1].split("&");
            for (String pair : pairs) {
                String[] leftRight = pair.split("=");
                paramMap.put(leftRight[0], leftRight[1]);
            }
        }

        if (!CollectionUtil.isNullOrEmpty(extParamMap)) {
            paramMap.putAll(extParamMap);
        }
        paramMap.put("useCursorFetch", "true");
        paramMap.put("rewriteBatchedStatements", "true");

        StringBuffer sb = new StringBuffer(dbUrl.length() + 128);
        sb.append(splits[0]).append("?");
        int index = 0;
        for (Map.Entry<String, String> entry : paramMap.entrySet()) {
            if (index != 0) {
                sb.append("&");
            }
            sb.append(entry.getKey()).append("=").append(entry.getValue());
            index++;
        }

        return sb.toString();
    }
}