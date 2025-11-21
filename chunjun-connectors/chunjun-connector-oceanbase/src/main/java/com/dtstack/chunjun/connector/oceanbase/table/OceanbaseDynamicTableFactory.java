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
package com.dtstack.chunjun.connector.oceanbase.table;

import com.dtstack.chunjun.connector.jdbc.options.JdbcCommonOptions;
import com.dtstack.chunjun.connector.jdbc.table.JdbcDynamicTableFactory;
import com.dtstack.chunjun.connector.oceanbase.config.OceanBaseMode;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Set;

public class OceanbaseDynamicTableFactory
        implements DynamicTableSourceFactory, DynamicTableSinkFactory {
    private static final String IDENTIFIER = "oceanbase-x";
    private JdbcDynamicTableFactory factory;

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        try {
            JdbcDynamicTableFactory dynamicTableSourceProxy = getDynamicTableSourceProxy(context);
            return dynamicTableSourceProxy.createDynamicTableSource(context);
        } catch (Exception e) {
            throw new RuntimeException("Create DynamicTableSource failed ", e);
        }
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        try {
            JdbcDynamicTableFactory dynamicTableSourceProxy = getDynamicTableSourceProxy(null);
            return dynamicTableSourceProxy.requiredOptions();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        try {
            JdbcDynamicTableFactory dynamicTableSourceProxy = getDynamicTableSourceProxy(null);
            return dynamicTableSourceProxy.optionalOptions();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        try {
            JdbcDynamicTableFactory dynamicTableSourceProxy = getDynamicTableSourceProxy(context);
            return dynamicTableSourceProxy.createDynamicTableSink(context);
        } catch (Exception e) {
            throw new RuntimeException("Create DynamicTableSink failed", e);
        }
    }

    private JdbcDynamicTableFactory getDynamicTableSourceProxy(Context context) throws Exception {
        if (factory == null) {
            if (context == null) {
                return new MysqlDynamicTableFactoryProxy();
            }
            OceanBaseMode mode;
            try {
                mode = getCompatibilityMode(context);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            if (mode == OceanBaseMode.MYSQL) {
                factory = new MysqlDynamicTableFactoryProxy();
            } else if (mode == OceanBaseMode.ORACLE) {
                factory = new OracleDynamicTableFactoryProxy();
            } else {
                throw new Exception("Nonsupport such mode " + mode);
            }
        }
        return factory;
    }

    private OceanBaseMode getCompatibilityMode(DynamicTableFactory.Context context) {
        Map<String, String> options = context.getCatalogTable().getOptions();
        String password = options.get(JdbcCommonOptions.PASSWORD.key());
        String url = options.get(JdbcCommonOptions.URL.key());
        String username = options.get(JdbcCommonOptions.USERNAME.key());
        String mode;
        try {
            Class.forName("com.oceanbase.jdbc.Driver");

            Connection connection = DriverManager.getConnection(url, username, password);
            Statement sm = connection.createStatement();
            ResultSet rs = sm.executeQuery("SHOW GLOBAL VARIABLES like 'ob_compatibility_mode'");
            rs.next();
            mode = rs.getString(2);
        } catch (ClassNotFoundException | SQLException e) {
            throw new RuntimeException("Get oceanbase compatibility mode failed", e);
        }
        return OceanBaseMode.valueOf(mode);
    }
}
