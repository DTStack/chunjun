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

package com.dtstack.flinkx.connector.sqlserver.source;

import com.dtstack.flinkx.conf.SyncConf;
import com.dtstack.flinkx.connector.jdbc.source.JdbcInputFormatBuilder;
import com.dtstack.flinkx.connector.jdbc.source.JdbcSourceFactory;
import com.dtstack.flinkx.connector.jdbc.util.JdbcUtil;
import com.dtstack.flinkx.connector.sqlserver.dialect.SqlserverDialect;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.commons.lang3.StringUtils;

/**
 * Company：www.dtstack.com
 *
 * @author shitou
 * @date 2021/5/19 15:31
 */
public class SqlserverSourceFactory extends JdbcSourceFactory {

    public SqlserverSourceFactory(SyncConf syncConf, StreamExecutionEnvironment env) {
        super(syncConf, env, null);
        super.jdbcDialect =
                new SqlserverDialect(
                        jdbcConf.isWithNoLock(),
                        jdbcConf.getJdbcUrl().startsWith("jdbc:jtds:sqlserver"));
    }

    @Override
    protected JdbcInputFormatBuilder getBuilder() {
        return new JdbcInputFormatBuilder(new SqlserverInputFormat());
    }

    /** table字段有可能是[schema].[table]格式 需要转换为对应的schema 和 table 字段* */
    @Override
    protected void resetTableInfo() {
        if (jdbcConf.getTable().startsWith("[")
                && jdbcConf.getTable().endsWith("]")
                && StringUtils.isBlank(jdbcConf.getSchema())) {
            JdbcUtil.resetSchemaAndTable(jdbcConf, "\\[", "\\]");
        }
    }
}
