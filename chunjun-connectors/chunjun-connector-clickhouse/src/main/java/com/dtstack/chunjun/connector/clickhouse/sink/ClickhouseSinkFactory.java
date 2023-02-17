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

package com.dtstack.chunjun.connector.clickhouse.sink;

import com.dtstack.chunjun.config.SyncConfig;
import com.dtstack.chunjun.connector.clickhouse.dialect.ClickhouseDialect;
import com.dtstack.chunjun.connector.clickhouse.util.ClickhouseUtil;
import com.dtstack.chunjun.connector.jdbc.sink.JdbcOutputFormatBuilder;
import com.dtstack.chunjun.connector.jdbc.sink.JdbcSinkFactory;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;

import java.sql.Connection;
import java.sql.SQLException;

public class ClickhouseSinkFactory extends JdbcSinkFactory {

    public ClickhouseSinkFactory(SyncConfig syncConfig) {
        super(syncConfig, new ClickhouseDialect());
    }

    @Override
    protected JdbcOutputFormatBuilder getBuilder() {
        return new ClickhouseOutputFormatBuilder(new ClickhouseOutputFormat());
    }

    @Override
    protected Connection getConn() {
        try {
            return ClickhouseUtil.getConnection(
                    jdbcConfig.getJdbcUrl(), jdbcConfig.getUsername(), jdbcConfig.getPassword());
        } catch (SQLException e) {
            throw new ChunJunRuntimeException(
                    String.format(
                            "failed to get clickhouse jdbc connection,jdbcUrl=%s",
                            jdbcConfig.getJdbcUrl()),
                    e);
        }
    }
}
