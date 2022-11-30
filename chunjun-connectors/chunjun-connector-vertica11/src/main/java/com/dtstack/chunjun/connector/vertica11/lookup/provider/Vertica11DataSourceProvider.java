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

package com.dtstack.chunjun.connector.vertica11.lookup.provider;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.spi.DataSourceProvider;

import javax.sql.DataSource;

import java.util.Map;

public class Vertica11DataSourceProvider implements DataSourceProvider {
    @Override
    public DataSource getDataSource(JsonObject json) {

        final HikariConfig config = new HikariConfig();

        for (Map.Entry<String, Object> entry : json) {
            switch (entry.getKey()) {
                case "url":
                    config.setJdbcUrl((String) entry.getValue());
                    break;
                case "username":
                    config.setUsername((String) entry.getValue());
                    break;
                case "password":
                    config.setPassword((String) entry.getValue());
                    break;
                case "maxActive":
                    config.setMaximumPoolSize((Integer) entry.getValue());
                    break;
                case "driverClassName":
                    config.setDriverClassName((String) entry.getValue());
                    break;
            }
        }

        return new HikariDataSource(config);
    }

    @Override
    public int maximumPoolSize(DataSource dataSource, JsonObject config) {
        if (dataSource instanceof HikariDataSource) {
            return ((HikariDataSource) dataSource).getMaximumPoolSize();
        }
        return -1;
    }

    @Override
    public void close(DataSource dataSource) {
        if (dataSource instanceof HikariDataSource) {
            ((HikariDataSource) dataSource).close();
        }
    }
}
