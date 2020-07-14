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
package com.dtstack.flinkx.phoenix5.util;

import com.dtstack.flinkx.rdb.DatabaseInterface;
import com.dtstack.flinkx.reader.MetaColumn;
import com.dtstack.flinkx.util.ClassUtil;
import com.dtstack.flinkx.util.TelnetUtil;
import org.codehaus.commons.compiler.CompileException;
import org.codehaus.janino.ClassBodyEvaluator;

import java.io.IOException;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;

/**
 * Date: 2020/02/28
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public class PhoenixUtil {

    public interface IPhoenixConn{
        Connection getConn(String url, String userName, String password)throws SQLException;
        Connection getConn(String url)throws SQLException;
    }

    public interface IPhoenixDbUtil{
        List<String> analyzeTable(String dbUrl, String username, String password, DatabaseInterface databaseInterface,
                                  String table, List<MetaColumn> metaColumns);
    }


    public static Connection getConnectionInternal(String url, String username, String password, ClassLoader parentClassLoader) throws SQLException, IOException, CompileException, IllegalAccessException, InstantiationException {
        Connection dbConn;
        synchronized (ClassUtil.LOCK_STR){
            DriverManager.setLoginTimeout(10);

            // telnet
            TelnetUtil.telnet(url);
            ClassBodyEvaluator cbe = new ClassBodyEvaluator();
            cbe.setParentClassLoader(parentClassLoader);
            cbe.setDefaultImports(new String[]{"java.sql.Connection", "java.sql.DriverManager", "java.sql.SQLException"});
            cbe.setImplementedInterfaces(new Class[]{IPhoenixConn.class});
            if (username == null) {
                StringReader sr = new StringReader("public Connection getConn(String url) throws SQLException { return DriverManager.getConnection(url); }");
                IPhoenixConn iPhoenixConn = (IPhoenixConn) cbe.createInstance(sr);
                dbConn = iPhoenixConn.getConn(url);
            } else {
                StringReader sr = new StringReader("public Connection getConn(String url, String userName, String password) throws SQLException { return DriverManager.getConnection(url, userName, password); }");
                IPhoenixConn iPhoenixConn = (IPhoenixConn) cbe.createInstance(sr);
                dbConn = iPhoenixConn.getConn(url, username, password);
            }
        }

        return dbConn;
    }

    public static List<String> getAnalyzeTable(String dbUrl, String username, String password, DatabaseInterface databaseInterface,
                                             String table, List<MetaColumn> metaColumns, ClassLoader parentClassLoader) throws IOException, CompileException {
        ClassBodyEvaluator cbe = new ClassBodyEvaluator();
        cbe.setParentClassLoader(parentClassLoader);
        cbe.setDefaultImports(new String[]{"com.dtstack.flinkx.rdb.util.DbUtil"});
        cbe.setImplementedInterfaces(new Class[]{IPhoenixDbUtil.class});
        StringReader sr = new StringReader("    public static List<String> analyzeTable(String dbUrl, String username, String password, DatabaseInterface databaseInterface,\n" +
                "                                            String table, List<MetaColumn> metaColumns) {\n" +
                "        List<String> ret = new ArrayList<>(metaColumns.size());\n" +
                "        Connection dbConn = null;\n" +
                "        Statement stmt = null;\n" +
                "        ResultSet rs = null;\n" +
                "        try {\n" +
                "            dbConn = PhoenixUtil.getConnectionInternal(dbUrl, username, password, childFirstClassLoader);\n" +
                "            if (null == dbConn) {\n" +
                "                throw new RuntimeException(\"Get hive connection error\");\n" +
                "            }\n" +
                "\n" +
                "            stmt = dbConn.createStatement();\n" +
                "            rs = stmt.executeQuery(databaseInterface.getSqlQueryFields(databaseInterface.quoteTable(table)));\n" +
                "            ResultSetMetaData rd = rs.getMetaData();\n" +
                "\n" +
                "            Map<String,String> nameTypeMap = new HashMap<>((rd.getColumnCount() << 2) / 3);\n" +
                "            for(int i = 0; i < rd.getColumnCount(); ++i) {\n" +
                "                nameTypeMap.put(rd.getColumnName(i+1),rd.getColumnTypeName(i+1));\n" +
                "            }\n" +
                "\n" +
                "            for (MetaColumn metaColumn : metaColumns) {\n" +
                "                if(metaColumn.getValue() != null){\n" +
                "                    ret.add(\"string\");\n" +
                "                } else {\n" +
                "                    ret.add(nameTypeMap.get(metaColumn.getName()));\n" +
                "                }\n" +
                "            }\n" +
                "        } catch (SQLException e) {\n" +
                "            throw new RuntimeException(e);\n" +
                "        } finally {\n" +
                "            closeDbResources(rs, stmt, dbConn, false);\n" +
                "        }\n" +
                "\n" +
                "        return ret;\n" +
                "    }");
        IPhoenixDbUtil iPhoenixDbUtil = (IPhoenixDbUtil) cbe.createInstance(sr);
        return iPhoenixDbUtil.analyzeTable(dbUrl, username, password,databaseInterface,table,metaColumns);
    }

}
