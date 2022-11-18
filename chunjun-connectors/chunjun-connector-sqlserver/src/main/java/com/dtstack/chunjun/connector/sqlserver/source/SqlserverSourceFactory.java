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

package com.dtstack.chunjun.connector.sqlserver.source;

import com.dtstack.chunjun.config.SyncConfig;
import com.dtstack.chunjun.connector.jdbc.source.JdbcInputFormatBuilder;
import com.dtstack.chunjun.connector.jdbc.source.JdbcSourceFactory;
import com.dtstack.chunjun.connector.jdbc.util.JdbcUtil;
import com.dtstack.chunjun.connector.sqlserver.dialect.SqlserverDialect;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.commons.lang3.StringUtils;

public class SqlserverSourceFactory extends JdbcSourceFactory {

    public SqlserverSourceFactory(SyncConfig syncConfig, StreamExecutionEnvironment env) {
        super(syncConfig, env, null);
        super.jdbcDialect =
                new SqlserverDialect(
                        jdbcConfig.isWithNoLock(),
                        jdbcConfig.getJdbcUrl().startsWith("jdbc:jtds:sqlserver"));
    }

    @Override
    protected JdbcInputFormatBuilder getBuilder() {
        return new JdbcInputFormatBuilder(new SqlserverInputFormat());
    }

    /** table字段有可能是[schema].[table]格式 需要转换为对应的schema 和 table 字段* */
    @Override
    protected void rebuildJdbcConf() {
        if (jdbcConfig.getTable().startsWith("[")
                && jdbcConfig.getTable().endsWith("]")
                && StringUtils.isBlank(jdbcConfig.getSchema())) {
            JdbcUtil.resetSchemaAndTable(jdbcConfig, "\\[", "\\]");
        } else {
            super.rebuildJdbcConf();
        }
    }
}
