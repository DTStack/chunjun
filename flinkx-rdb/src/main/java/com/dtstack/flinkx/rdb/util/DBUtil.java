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

import com.dtstack.flinkx.enums.EDatabaseType;
import com.dtstack.flinkx.rdb.DatabaseInterface;
import com.dtstack.flinkx.rdb.ParameterValuesProvider;
import com.dtstack.flinkx.rdb.type.TypeConverterInterface;
import com.dtstack.flinkx.util.ClassUtil;
import com.dtstack.flinkx.util.DateUtil;
import com.dtstack.flinkx.util.SysUtil;
import com.dtstack.flinkx.util.TelnetUtil;
import org.apache.flink.types.Row;

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * Utilities for relational database connection and sql execution
 * company: www.dtstack.com
 * @author huyifan_zju@
 */
public class DBUtil {

    private static int MAX_RETRY_TIMES = 3;

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
        if (!url.startsWith("jdbc:mysql")) {
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
            DBUtil.closeDBResources(res, statement, null);
        }
        return result;
    }

    public static void closeDBResources(ResultSet rs, Statement stmt,
                                        Connection conn) {
        if (null != rs) {
            try {
                rs.close();
            } catch (SQLException unused) {
            }
        }

        if (null != stmt) {
            try {
                stmt.close();
            } catch (SQLException unused) {
            }
        }

        if (null != conn) {
            try {
                conn.close();
            } catch (SQLException unused) {
            }
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
            e.printStackTrace();
        }
    }

    public static void executeOneByOne(Connection dbConn, List<String> sqls) {
        if(sqls == null || sqls.size() == 0) {
            return;
        }

        try {
            Statement stmt = dbConn.createStatement();
            for(String sql : sqls) {
                stmt.execute(sql);
            }
        } catch (SQLException e) {
            e.printStackTrace();
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
                                            String table,List<String> column) {
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

            for (String col : column) {
                ret.add(nameTypeMap.get(col));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            closeDBResources(rs,stmt,dbConn);
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

    public static void getRow(EDatabaseType dbType, Row row, List<String> descColumnTypeList, ResultSet resultSet,
                              TypeConverterInterface typeConverter) throws SQLException{
        for (int pos = 0; pos < row.getArity(); pos++) {
            Object obj = resultSet.getObject(pos + 1);
            if(obj != null) {
                if (EDatabaseType.Oracle == dbType) {
                    if((obj instanceof java.util.Date || obj.getClass().getSimpleName().toUpperCase().contains("TIMESTAMP")) ) {
                        obj = resultSet.getTimestamp(pos + 1);
                    }
                } else if(EDatabaseType.MySQL == dbType) {
                    if(descColumnTypeList != null && descColumnTypeList.size() != 0) {
                        if(descColumnTypeList.get(pos).equalsIgnoreCase("year")) {
                            java.util.Date date = (java.util.Date) obj;
                            String year = DateUtil.dateToYearString(date);
                            System.out.println(year);
                            obj = year;
                        } else if(descColumnTypeList.get(pos).equalsIgnoreCase("tinyint")) {
                            if(obj instanceof Boolean) {
                                obj = ((Boolean) obj ? 1 : 0);
                            }
                        } else if(descColumnTypeList.get(pos).equalsIgnoreCase("bit")) {
                            if(obj instanceof Boolean) {
                                obj = ((Boolean) obj ? 1 : 0);
                            }
                        }
                    }
                } else if(EDatabaseType.SQLServer == dbType) {
                    if(descColumnTypeList != null && descColumnTypeList.size() != 0) {
                        if(descColumnTypeList.get(pos).equalsIgnoreCase("bit")) {
                            if(obj instanceof Boolean) {
                                obj = ((Boolean) obj ? 1 : 0);
                            }
                        }
                    }
                } else if(EDatabaseType.PostgreSQL == dbType){
                    if(descColumnTypeList != null && descColumnTypeList.size() != 0) {
                        obj = typeConverter.convert(obj,descColumnTypeList.get(pos));
                    }
                }
            }

            row.setField(pos, obj);
        }
    }

    public static String getQuerySql(DatabaseInterface databaseInterface,String table,List<String> column,
                                     String splitKey,String where,boolean isSplitByKey) {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ").append(databaseInterface.quoteColumns(column)).append(" FROM ");
        sb.append(databaseInterface.quoteTable(table));

        StringBuilder filter = new StringBuilder();

        if(isSplitByKey) {
            filter.append(databaseInterface.getSplitFilter(splitKey));
        }

        if(where != null && where.trim().length() != 0) {
            if(filter.length() > 0) {
                filter.append(" AND ");
            }
            filter.append(where);
        }

        if(filter.length() != 0) {
            sb.append(" WHERE ").append(filter);
        }

        return sb.toString();
    }

    public static String formatJdbcUrl(String pluginName,String dbUrl){
        if(pluginName.equalsIgnoreCase("mysqlreader")
                || pluginName.equalsIgnoreCase("mysqldreader")
                || pluginName.equalsIgnoreCase("postgresqlreader")){
            String[] splits = dbUrl.split("\\?");

            Map<String,String> paramMap = new HashMap<String,String>();
            if(splits.length > 1) {
                String[] pairs = splits[1].split("&");
                for(String pair : pairs) {
                    String[] leftRight = pair.split("=");
                    paramMap.put(leftRight[0], leftRight[1]);
                }
            }

            paramMap.put("useCursorFetch", "true");

            if(pluginName.equalsIgnoreCase("mysqlreader")
                    || pluginName.equalsIgnoreCase("mysqldreader")){
                paramMap.put("zeroDateTimeBehavior","convertToNull");
            }

            StringBuffer sb = new StringBuffer(splits[0]);
            if(paramMap.size() != 0) {
                sb.append("?");
                int index = 0;
                for(Map.Entry<String,String> entry : paramMap.entrySet()) {
                    if(index != 0) {
                        sb.append("&");
                    }
                    sb.append(entry.getKey() + "=" + entry.getValue());
                    index++;
                }
            }

            dbUrl = sb.toString();
        }

        return dbUrl;
    }
}
