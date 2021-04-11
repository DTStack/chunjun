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

import com.dtstack.flinkx.connector.jdbc.lookup.JdbcLruTableFunction;
import com.dtstack.flinkx.lookup.options.LookupOptions;
import io.vertx.core.json.JsonObject;

import org.apache.flink.connector.jdbc.internal.options.JdbcOptions;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import static com.dtstack.flinkx.connector.jdbc.constants.JdbcLookUpConstants.DEFAULT_IDLE_CONNECTION_TEST_PEROID;
import static com.dtstack.flinkx.connector.jdbc.constants.JdbcLookUpConstants.DEFAULT_TEST_CONNECTION_ON_CHECKIN;
import static com.dtstack.flinkx.connector.jdbc.constants.JdbcLookUpConstants.DT_PROVIDER_CLASS;
import static com.dtstack.flinkx.connector.jdbc.constants.JdbcLookUpConstants.PREFERRED_TEST_QUERY_SQL;
import static com.dtstack.flinkx.connector.mysql.constants.MysqlConstants.MYSQL_DRIVER;

/**
 * @author chuixue
 * @create 2021-04-10 21:53
 * @description
 **/
public class MysqlLruTableFunction extends JdbcLruTableFunction {
    public MysqlLruTableFunction(
            JdbcOptions options,
            LookupOptions lookupOptions,
            String[] fieldNames,
            DataType[] fieldTypes,
            String[] keyNames, RowType rowType) {
        super(options, lookupOptions, fieldNames, fieldTypes, keyNames, rowType);
    }

    @Override
    public JsonObject buildJdbcConfig() {
        JsonObject mysqlClientConfig = new JsonObject();
        mysqlClientConfig.put("url", options.getDbURL())
                .put("driver_class", MYSQL_DRIVER)
                .put("max_pool_size", asyncPoolSize)
                .put("user", options.getUsername().get())
                .put("password", options.getPassword().get())
                .put("provider_class", DT_PROVIDER_CLASS.defaultValue())
                .put("preferred_test_query", PREFERRED_TEST_QUERY_SQL.defaultValue())
                .put(
                        "idle_connection_test_period",
                        DEFAULT_IDLE_CONNECTION_TEST_PEROID.defaultValue())
                .put(
                        "test_connection_on_checkin",
                        DEFAULT_TEST_CONNECTION_ON_CHECKIN.defaultValue());

        return mysqlClientConfig;

    }
}
