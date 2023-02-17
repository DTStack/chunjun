/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.dtstack.chunjun.connector.vertica11.lookup;

import com.dtstack.chunjun.connector.jdbc.config.JdbcConfig;
import com.dtstack.chunjun.connector.jdbc.dialect.JdbcDialect;
import com.dtstack.chunjun.connector.jdbc.lookup.JdbcLruTableFunction;
import com.dtstack.chunjun.lookup.config.LookupConfig;

import org.apache.flink.table.types.logical.RowType;

import io.vertx.core.json.JsonObject;

import java.util.Map;

import static com.dtstack.chunjun.connector.vertica11.lookup.options.Vertica11LookupOptions.DT_PROVIDER_CLASS;

public class Vertica11LruTableFunction extends JdbcLruTableFunction {

    private static final long serialVersionUID = -6018243866809195735L;

    private final JdbcConfig jdbcConfig;

    private final JdbcDialect jdbcDialect;

    public Vertica11LruTableFunction(
            JdbcConfig jdbcConfig,
            JdbcDialect jdbcDialect,
            LookupConfig lookupConfig,
            String[] fieldNames,
            String[] keyNames,
            RowType rowType) {
        super(jdbcConfig, jdbcDialect, lookupConfig, fieldNames, keyNames, rowType);
        this.jdbcConfig = jdbcConfig;
        this.jdbcDialect = jdbcDialect;
    }

    @Override
    public JsonObject createJdbcConfig(Map<String, Object> druidConfMap) {
        JsonObject clientConfig = new JsonObject();
        clientConfig
                .put("url", jdbcConfig.getJdbcUrl())
                .put("username", jdbcConfig.getUsername())
                .put("password", jdbcConfig.getPassword())
                .put("driverClassName", jdbcDialect.defaultDriverName().orElse(""))
                .put("provider_class", DT_PROVIDER_CLASS.defaultValue())
                .put("maxActive", asyncPoolSize);

        return clientConfig;
    }
}
