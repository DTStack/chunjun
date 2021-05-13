/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.flinkx.oracle;

import java.sql.Connection;
import java.sql.SQLException;

public interface IOracleHelper {

    String CLASS_READER_STR =
            "    @Override\n" +
                    "   public Connection getConnection(String url, String user, String password) throws SQLException {\n" +
                    "        Connection dbConn;\n" +
                    "        synchronized (ClassUtil.LOCK_STR) {\n" +
                    "            DriverManager.setLoginTimeout(10);\n" +
                    "            dbConn = DriverManager.getConnection(url, user, password);\n" +
                    "        }\n" +
                    "\n" +
                    "        return dbConn;\n" +
                    "    }\n" +
                    "\n";

    String CLASS_WRITER_STR =
            "  @Override\n" +
                    "   public Connection getConnection(String url, String user, String password) throws SQLException {\n" +
                    "     return DbUtil.getConnection(url, user, password)  ;  \n" +
                    "    }\n" +
                    "\n";

    /**
     * 获取jdbc连接
     * @param url
     * @param user
     * @param password
     * @return
     * @throws SQLException
     */
    public Connection getConnection(String url, String user, String password) throws SQLException ;

}
