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

import com.dtstack.flinkx.rdb.DatabaseInterface;
import com.dtstack.flinkx.rdb.ParameterValuesProvider;
import com.dtstack.flinkx.reader.MetaColumn;
import com.dtstack.flinkx.util.ClassUtil;
import com.dtstack.flinkx.util.SysUtil;
import com.dtstack.flinkx.util.TelnetUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.util.CollectionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 *
 * Utilities for relational database connection and sql execution
 * company: www.dtstack.com
 * @author huyifan_zju@
 */
public class DBUtil {

    private static final Logger LOG = LoggerFactory.getLogger(DBUtil.class);

    private static int MAX_RETRY_TIMES = 3;

    private static int SECOND_LENGTH = 10;
    private static int MILLIS_LENGTH = 13;
    private static int MICRO_LENGTH = 16;
    private static int NANOS_LENGTH = 19;

    public static final Pattern DB_PATTERN = Pattern.compile("\\?");

    public static final String INCREMENT_FILTER_PLACEHOLDER = "${incrementFilter}";

    public static final String RESTORE_FILTER_PLACEHOLDER = "${restoreFilter}";

    public static final String TEMPORARY_TABLE_NAME = "flinkx_tmp";

    public static final String NULL_STRING = "null";

    private static Connection getConnectionInternal(String url, String username, String password) throws SQLException {
        Connection dbConn;
        synchronized (ClassUtil.lock_str){
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

    public static Connection getConnection(String url, String username, String password) throws SQLException {
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


    public static List<Map<String,Object>> executeQuery(Connection connection, String sql) {
        List<Map<String,Object>> result = com.google.common.collect.Lists.newArrayList();
        ResultSet res = null;
        Statement statement = null;
        try{
            statement = connection.createStatement();
            res =  statement.executeQuery(sql);
            int columns = res.getMetaData().getColumnCount();
            List<String> columnName = com.google.common.collect.Lists.newArrayList();
            for(int i = 0; i < columns; i++){
                columnName.add(res.getMetaData().getColumnName(i + 1));
            }

            while(res.next()){
                Map<String,Object> row = com.google.common.collect.Maps.newHashMap();
                for(int i = 0;i < columns; i++){
                    row.put(columnName.get(i), res.getObject(i + 1));
                }
                result.add(row);
            }
        }catch(Exception e){
            throw new RuntimeException(e);
        }
        finally{
            DBUtil.closeDBResources(res, statement, null, false);
        }
        return result;
    }

    public static void closeDBResources(ResultSet rs, Statement stmt,
                                        Connection conn, boolean commit) {
        if (null != rs) {
            try {
                LOG.info("Start close resultSet");
                rs.close();
                LOG.info("Close resultSet successful");
            } catch (SQLException e) {
                LOG.warn("Close resultSet error:{}",e);
            }
        }

        if (null != stmt) {
            try {
                LOG.info("Start close statement");
                stmt.close();
                LOG.info("Close statement successful");
            } catch (SQLException e) {
                LOG.warn("Close statement error:{}",e);
            }
        }

        if (null != conn) {
            try {
                if(commit){
                    commit(conn);
                }

                LOG.info("Start close connection");
                conn.close();
                LOG.info("Close connection successful");
            } catch (SQLException e) {
                LOG.warn("Close connection error:{}",e);
            }
        }
    }

    public static void commit(Connection conn){
        try {
            if (!conn.isClosed() && !conn.getAutoCommit()){
                LOG.info("Start commit connection");
                conn.commit();
                LOG.info("Commit connection successful");
            }
        } catch (SQLException e){
            LOG.warn("commit error:{}",e);
        }
    }

    public static void executeBatch(Connection dbConn, List<String> sqls) {
        if(sqls == null || sqls.size() == 0) {
            return;
        }

        try {
            Statement stmt = dbConn.createStatement();
            for(String sql : sqls) {
                stmt.addBatch(sql);
            }
            stmt.executeBatch();
        } catch (SQLException e) {
            throw new RuntimeException("execute batch sql error:{}",e);
        } finally {
            commit(dbConn);
        }
    }

    public static Map<String,List<String>> getPrimaryOrUniqueKeys(String table, Connection dbConn) throws SQLException {
        Map<String,List<String>> keyMap = new HashMap<>();
        DatabaseMetaData meta = dbConn.getMetaData();
        ResultSet rs = meta.getIndexInfo(null,null,table,true,false);
        while(rs.next()) {
            String pkName = rs.getString(6);
            String columnName = rs.getString(9);
            if(!keyMap.containsKey(pkName)) {
                keyMap.put(pkName, new ArrayList<>());
            }
            keyMap.get(pkName).add(columnName);
        }
        return keyMap;
    }

    public static Object[][] getParameterValues(final int channels){
        ParameterValuesProvider provider = new ParameterValuesProvider() {
            @Override
            public Serializable[][] getParameterValues() {
                Integer[][] parameters = new Integer[channels][];
                for(int i = 0; i < channels; ++i) {
                    parameters[i] = new Integer[2];
                    parameters[i][0] = channels;
                    parameters[i][1] = i;
                }
                return parameters;
            }
        };

        return provider.getParameterValues();
    }

    public static List<String> analyzeTable(String dbURL,String username,String password,DatabaseInterface databaseInterface,
                                            String table,List<MetaColumn> metaColumns) {
        List<String> ret = new ArrayList<>();
        Connection dbConn = null;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            dbConn = getConnection(dbURL, username, password);
            stmt = dbConn.createStatement();
            rs = stmt.executeQuery(databaseInterface.getSQLQueryFields(databaseInterface.quoteTable(table)));
            ResultSetMetaData rd = rs.getMetaData();

            Map<String,String> nameTypeMap = new HashMap<>();
            for(int i = 0; i < rd.getColumnCount(); ++i) {
                nameTypeMap.put(rd.getColumnName(i+1),rd.getColumnTypeName(i+1));
            }

            for (MetaColumn metaColumn : metaColumns) {
                if(metaColumn.getValue() != null){
                    ret.add("string");
                } else {
                    ret.add(nameTypeMap.get(metaColumn.getName()));
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            closeDBResources(rs, stmt, dbConn, false);
        }

        return ret;
    }

    public static void setParameterValue(Object param,PreparedStatement statement,int i) throws SQLException{
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
            throw new IllegalArgumentException("open() failed. Parameter " + i + " of type " + param.getClass() + " is not handled (yet)." );
        }
    }

    public static Object clobToString(Object obj) throws Exception{
        String dataStr;
        if(obj instanceof Clob){
            Clob clob = (Clob)obj;
            BufferedReader bf = new BufferedReader(clob.getCharacterStream());
            StringBuilder stringBuilder = new StringBuilder();
            String line;
            while ((line = bf.readLine()) != null){
                stringBuilder.append(line);
            }
            dataStr = stringBuilder.toString();
        } else {
            return obj;
        }

        return dataStr;
    }



    public static String getNanosTimeStr(String timeStr){
        if(timeStr.length() < 29){
            timeStr += StringUtils.repeat("0",29 - timeStr.length());
        }

        return timeStr;
    }

    public static int getNanos(long startLocation){
        String timeStr = String.valueOf(startLocation);
        int nanos;
        if (timeStr.length() == SECOND_LENGTH){
            nanos = 0;
        } else if (timeStr.length() == MILLIS_LENGTH){
            nanos = Integer.parseInt(timeStr.substring(SECOND_LENGTH,MILLIS_LENGTH)) * 1000000;
        } else if (timeStr.length() == MICRO_LENGTH){
            nanos = Integer.parseInt(timeStr.substring(SECOND_LENGTH,MICRO_LENGTH)) * 1000;
        } else if (timeStr.length() == NANOS_LENGTH){
            nanos = Integer.parseInt(timeStr.substring(SECOND_LENGTH,NANOS_LENGTH));
        } else {
            throw new IllegalArgumentException("Unknown time unit:startLocation=" + startLocation);
        }

        return nanos;
    }

    public static long getMillis(long startLocation){
        String timeStr = String.valueOf(startLocation);
        long millisSecond;
        if (timeStr.length() == SECOND_LENGTH){
            millisSecond = startLocation * 1000;
        } else if (timeStr.length() == MILLIS_LENGTH){
            millisSecond = startLocation;
        } else if (timeStr.length() == MICRO_LENGTH){
            millisSecond = startLocation / 1000;
        } else if (timeStr.length() == NANOS_LENGTH){
            millisSecond = startLocation / 1000000;
        } else {
            throw new IllegalArgumentException("Unknown time unit:startLocation=" + startLocation);
        }

        return millisSecond;
    }

    /**
     * 格式化jdbc连接
     * @param dbUrl         原jdbc连接
     * @param extParamMap   需要额外添加的参数
     * @return  格式化后jdbc连接URL字符串
     */
    public static String formatJdbcUrl(String dbUrl, Map<String,String> extParamMap){
//        if(pluginName.equalsIgnoreCase(PluginNameConstrant.MYSQL_WRITER)
//                || pluginName.equalsIgnoreCase(PluginNameConstrant.GBASE_WRITER) ){
        String[] splits = DB_PATTERN.split(dbUrl);

        Map<String,String> paramMap = new HashMap<String,String>();
        if(splits.length > 1) {
            String[] pairs = splits[1].split("&");
            for(String pair : pairs) {
                String[] leftRight = pair.split("=");
                paramMap.put(leftRight[0], leftRight[1]);
            }
        }

        if(!CollectionUtil.isNullOrEmpty(extParamMap)){
            paramMap.putAll(extParamMap);
        }
        paramMap.put("useCursorFetch", "true");
        paramMap.put("rewriteBatchedStatements", "true");

        StringBuffer sb = new StringBuffer(dbUrl.length() + 128);
        sb.append(splits[0]).append("?");
        int index = 0;
        for(Map.Entry<String,String> entry : paramMap.entrySet()) {
            if(index != 0) {
                sb.append("&");
            }
            sb.append(entry.getKey()).append("=").append(entry.getValue());
            index++;
        }

        return sb.toString();
    }
}
