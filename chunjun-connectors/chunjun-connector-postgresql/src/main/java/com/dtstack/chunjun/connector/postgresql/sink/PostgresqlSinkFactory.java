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

package com.dtstack.chunjun.connector.postgresql.sink;

import com.dtstack.chunjun.config.SyncConfig;
import com.dtstack.chunjun.connector.jdbc.dialect.JdbcDialect;
import com.dtstack.chunjun.connector.jdbc.sink.JdbcOutputFormatBuilder;
import com.dtstack.chunjun.connector.jdbc.sink.JdbcSinkFactory;
import com.dtstack.chunjun.connector.postgresql.dialect.PostgresqlDialect;

/** Starting with Postgresql that is for compatible with 1.10 API. */
public class PostgresqlSinkFactory extends JdbcSinkFactory {

    public PostgresqlSinkFactory(SyncConfig syncConfig) {
        super(syncConfig, new PostgresqlDialect());
    }

    public PostgresqlSinkFactory(SyncConfig syncConfig, JdbcDialect dialect) {
        super(syncConfig, dialect);
    }

    @Override
    protected JdbcOutputFormatBuilder getBuilder() {
        return new JdbcOutputFormatBuilder(new PostgresOutputFormat());
    }
}
