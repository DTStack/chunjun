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


package com.dtstack.flinkx.test.rdb;

import com.alibaba.fastjson.JSONArray;
import com.dtstack.flinkx.test.core.BaseTest;
import com.dtstack.flinkx.util.ClassUtil;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

/**
 * @author jiangbo
 * @date 2020/2/11
 */
public abstract class JdbcTest extends BaseTest {

    public static final String KEY_JDBC_URL = "jdbcUrl";
    public static final String KEY_USERNAME = "username";
    public static final String KEY_PASSWORD = "password";
    public static final String KEY_INIT_SQL = "initSql";

    protected Connection connection;

    @Override
    protected boolean isDataSourceValid() {
//        try {
//            ClassUtil.forName(getDriverName());
//            String jdbcUrl = connectionConfig.getJSONArray(KEY_JDBC_URL).getString(0);
//            String username = connectionConfig.getString(KEY_USERNAME);
//            String password = connectionConfig.getString(KEY_PASSWORD);
//            connection = DriverManager.getConnection(jdbcUrl, username, password);
//            return true;
//        } catch (Exception e) {
//            LOG.error("获取数据源连接出错：", e);
//        }

        return false;
    }

    @Override
    protected void prepareDataInternal() throws Exception {
        JSONArray sqlArray = actionBeforeTest.getJSONArray(getPluginType());
        executeBatchSql(sqlArray);
    }

    private void executeBatchSql(JSONArray initSqlArray) throws Exception {
        if (initSqlArray == null || initSqlArray.isEmpty()) {
            return;
        }

        Statement statement;
        try {
            statement = connection.createStatement();
        } catch (Exception e) {
            throw new RuntimeException("创建[statement]出错：", e);
        }

        for (Object sql : initSqlArray) {
            if (sql == null || StringUtils.isEmpty(sql.toString())) {
                continue;
            }

            try {
                statement.execute(sql.toString());
            } catch (Exception e) {
                statement.close();
                LOG.error("执行sql[{}]出错", sql);
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    protected void cleanData() throws Exception {
        JSONArray sqlArray = actionAfterTest.getJSONArray(getPluginType());
        executeBatchSql(sqlArray);
        closeConnection();
    }

    private void closeConnection() {
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (Exception e) {
            LOG.warn("关闭数据源连接出错：", e);
        }
    }

    protected abstract String getDriverName();

    protected abstract String getPluginType();
}
