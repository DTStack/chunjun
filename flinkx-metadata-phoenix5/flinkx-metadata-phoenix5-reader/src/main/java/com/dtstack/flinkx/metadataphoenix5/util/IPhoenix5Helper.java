/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.flinkx.metadataphoenix5.util;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Date: 2020/10/10
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public interface IPhoenix5Helper {

    String CLASS_STR = "public transient RowProjector rowProjector;\n" +
            "    public List<PDataType> instanceList;\n" +
            "\n" +
            "    @Override\n" +
            "    public Connection getConn(String url, Properties properties) throws SQLException {\n" +
            "        Connection dbConn;\n" +
            "        synchronized (ClassUtil.LOCK_STR) {\n" +
            "            DriverManager.setLoginTimeout(10);\n" +
            "            // telnet\n" +
            "            TelnetUtil.telnet(url);\n" +
            "            dbConn = DriverManager.getConnection(url, properties);\n" +
            "        }\n" +
            "\n" +
            "        return dbConn;\n" +
            "    }\n" +
            "\n";

    /**
     * 获取phoenix jdbc连接
     * @param url url地址
     * @param properties 连接配置
     * @return 连接
     * @throws SQLException sql异常
     */
    Connection getConn(String url, Properties properties) throws SQLException;

}
