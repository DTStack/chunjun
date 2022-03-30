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

package com.dtstack.flinkx.connector.postgresql.source;

import com.dtstack.flinkx.conf.SyncConf;
import com.dtstack.flinkx.connector.jdbc.source.JdbcInputFormatBuilder;
import com.dtstack.flinkx.connector.jdbc.source.JdbcSourceFactory;
import com.dtstack.flinkx.connector.postgresql.dialect.PostgresqlDialect;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Starting with Postgresql that is for compatible with 1.10 API.
 *
 * @program: flinkx
 * @author: wuren
 * @create: 2021/04/28
 */
public class PostgresqlSourceFactory extends JdbcSourceFactory {

    public PostgresqlSourceFactory(SyncConf syncConf, StreamExecutionEnvironment env) {
        super(syncConf, env, new PostgresqlDialect());
    }

    @Override
    protected JdbcInputFormatBuilder getBuilder() {
        return new JdbcInputFormatBuilder(new PostgresqlInputFormat());
    }
}
