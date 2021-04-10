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

package com.dtstack.flinkx.connector.mysql.lookup;

import org.apache.flink.connector.jdbc.internal.options.JdbcOptions;
import org.apache.flink.table.types.logical.RowType;

import com.dtstack.flinkx.connector.jdbc.lookup.JdbcRowDataLookupFunction;
import com.dtstack.flinkx.lookup.options.LookupOptions;
import com.dtstack.flinkx.util.DtStringUtil;
import com.google.common.collect.Maps;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Map;

/**
 * @author chuixue
 * @create 2021-04-10 14:06
 * @description
 **/
public class MysqlRowDataLookupFunction extends JdbcRowDataLookupFunction {
    private static final String MYSQL_DRIVER = "com.mysql.jdbc.Driver";

    public MysqlRowDataLookupFunction(
            JdbcOptions options,
            LookupOptions lookupOptions,
            String[] fieldNames,
            String[] keyNames, RowType rowType) {
        super(options, lookupOptions, fieldNames, keyNames, rowType);
    }

    @Override
    public Connection getConn(String dbUrl, String userName, String password) {
        try {
            Class.forName(MYSQL_DRIVER);
            //add param useCursorFetch=true
            Map<String, String> addParams = Maps.newHashMap();
            addParams.put("useCursorFetch", "true");
            String targetDbUrl = DtStringUtil.addJdbcParam(dbUrl, addParams, true);
            return DriverManager.getConnection(targetDbUrl, userName, password);
        } catch (Exception e) {
            throw new RuntimeException("", e);
        }
    }
}
