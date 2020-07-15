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
import com.dtstack.flinkx.rdb.util.DbUtil;
import com.dtstack.flinkx.reader.MetaColumn;
import com.dtstack.flinkx.util.ClassUtil;
import com.dtstack.flinkx.util.TelnetUtil;
import org.codehaus.commons.compiler.CompileException;
import org.codehaus.janino.ClassBodyEvaluator;

import javax.ws.rs.NotSupportedException;
import java.io.IOException;
import java.io.StringReader;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Date: 2020/02/28
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public class PhoenixUtil {

    public interface IPhoenixConn {
        default Connection getConn(String url, String userName, String password) throws SQLException {
            throw new NotSupportedException("this method must be override");
        }

        default Connection getConn(String url) throws SQLException {
            throw new NotSupportedException("this method must be override");
        }
    }

    public interface IPhoenixDbUtil {
        default List<String> analyzeTable(String dbUrl, String username, String password, DatabaseInterface databaseInterface,
                                          String table, List<MetaColumn> metaColumns) {
            throw new NotSupportedException("this method must be override");
        }
    }


    public static Connection getConnectionInternal(String url, String username, String password, ClassLoader parentClassLoader) throws SQLException, IOException, CompileException, IllegalAccessException, InstantiationException {
        Connection dbConn;
        synchronized (ClassUtil.LOCK_STR) {
            DriverManager.setLoginTimeout(10);

            // telnet
            TelnetUtil.telnet(url);
            ClassBodyEvaluator cbe = new ClassBodyEvaluator();
            cbe.setParentClassLoader(parentClassLoader);
            cbe.setDefaultImports("java.sql.Connection", "java.sql.DriverManager", "java.sql.SQLException");
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
        cbe.setDefaultImports("com.dtstack.flinkx.rdb.util.DbUtil", "com.dtstack.flinkx.phoenix5.util.PhoenixUtil");
        cbe.setImplementedInterfaces(new Class[]{IPhoenixDbUtil.class});
        StringReader sr = new StringReader(
                "public List<String> analyzeTable" +
                        "(String dbUrl, String username, String password, DatabaseInterface databaseInterface, String table, List<MetaColumn> metaColumns) " +
                        "{ return PhoenixUtil.analyzeTable(dbUrl, username, password, databaseInterface, table, metaColumns);}");
        IPhoenixDbUtil iPhoenixDbUtil = (IPhoenixDbUtil) cbe.createInstance(sr);
        return iPhoenixDbUtil.analyzeTable(dbUrl, username, password, databaseInterface, table, metaColumns);
    }

    public static List<String> analyzeTable(String dbUrl, String username, String password, DatabaseInterface
            databaseInterface, String table, List<MetaColumn> metaColumns, ClassLoader childFirstClassLoader) {
        Connection dbConn;

        try {
            dbConn = PhoenixUtil.getConnectionInternal(dbUrl, username, password, childFirstClassLoader);

            if (dbConn == null) {
                throw new RuntimeException("Phoenix get conn error");
            }

            return DbUtil.analyzeTableFromConn(dbConn, databaseInterface, table, metaColumns);
        } catch (IOException | InstantiationException | CompileException | SQLException | IllegalAccessException e) {
            throw new RuntimeException("Phoenix analyzeTable error ", e);
        }
    }
}
