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

package com.dtstack.chunjun.connector.greenplum.sink;

import com.dtstack.chunjun.config.SyncConfig;
import com.dtstack.chunjun.connector.greenplum.dialect.GreenplumDialect;
import com.dtstack.chunjun.connector.jdbc.sink.JdbcOutputFormatBuilder;
import com.dtstack.chunjun.connector.jdbc.sink.JdbcSinkFactory;
import com.dtstack.chunjun.connector.postgresql.dialect.PostgresqlDialect;

import org.apache.commons.lang3.StringUtils;

import static com.dtstack.chunjun.connector.greenplum.sink.GreenplumOutputFormat.INSERT_SQL_MODE_TYPE;

public class GreenplumSinkFactory extends JdbcSinkFactory {

    public GreenplumSinkFactory(SyncConfig syncConfig) {
        super(syncConfig, null);
        if (syncConfig.getWriter().getParameter().get("insertSqlMode") != null
                && INSERT_SQL_MODE_TYPE.equalsIgnoreCase(
                        syncConfig.getWriter().getParameter().get("insertSqlMode").toString())) {
            this.jdbcDialect = new PostgresqlDialect();
            String pgUrl = changeToPostgresqlUrl(this.jdbcConfig.getJdbcUrl());
            this.jdbcConfig.setJdbcUrl(pgUrl);
        } else {
            this.jdbcDialect = new GreenplumDialect();
        }
    }

    @Override
    protected JdbcOutputFormatBuilder getBuilder() {
        return new JdbcOutputFormatBuilder(new GreenplumOutputFormat());
    }

    private String changeToPostgresqlUrl(String gpUrl) {
        String pgUrl =
                StringUtils.replaceOnce(
                        gpUrl, GreenplumDialect.URL_START, PostgresqlDialect.URL_START);
        pgUrl = StringUtils.replaceOnce(pgUrl, GreenplumDialect.DATABASE_NAME, "/");
        return pgUrl;
    }
}
