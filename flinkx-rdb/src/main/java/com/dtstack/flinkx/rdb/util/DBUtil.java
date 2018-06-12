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

import com.dtstack.flinkx.util.ClassUtil;
import com.dtstack.flinkx.util.SysUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DatabaseMetaData;

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

            if (username == null) {
                dbConn = DriverManager.getConnection(url);
            } else {
                dbConn = DriverManager.getConnection(url, username, password);
            }
        }

        return dbConn;
    }

    public static Connection getConnection(String url, String username, String password) throws SQLException {
        Connection dbConn = getConnectionInternal(url, username, password);
        boolean failed = true;
        if (url.startsWith("jdbc:mysql")) {
            for(int i = 0; i < MAX_RETRY_TIMES && failed; ++i) {
                try {
                    dbConn.createStatement().execute("select 111");
                    failed = false;
                } catch(SQLException e) {
                    if(i == MAX_RETRY_TIMES) {
                        throw e;
                    } else {
                        SysUtil.sleep(3000);
                    }
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
}
