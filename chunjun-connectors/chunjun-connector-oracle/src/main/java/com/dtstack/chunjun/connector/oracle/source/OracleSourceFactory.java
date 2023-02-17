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

package com.dtstack.chunjun.connector.oracle.source;

import com.dtstack.chunjun.config.SyncConfig;
import com.dtstack.chunjun.connector.jdbc.source.JdbcInputFormat;
import com.dtstack.chunjun.connector.jdbc.source.JdbcInputFormatBuilder;
import com.dtstack.chunjun.connector.jdbc.source.JdbcSourceFactory;
import com.dtstack.chunjun.connector.jdbc.util.JdbcUtil;
import com.dtstack.chunjun.connector.oracle.dialect.OracleDialect;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

public class OracleSourceFactory extends JdbcSourceFactory {

    public OracleSourceFactory(SyncConfig syncConfig, StreamExecutionEnvironment env) {
        super(syncConfig, env, new OracleDialect());
    }

    @Override
    protected JdbcInputFormatBuilder getBuilder() {
        return new JdbcInputFormatBuilder(new JdbcInputFormat());
    }

    @Override
    protected void rebuildJdbcConf() {
        super.rebuildJdbcConf();

        Properties properties = new Properties();
        if (jdbcConfig.getConnectTimeOut() != 0) {
            properties.put(
                    "oracle.jdbc.ReadTimeout",
                    String.valueOf(jdbcConfig.getConnectTimeOut() * 1000));
            properties.put(
                    "oracle.net.CONNECT_TIMEOUT",
                    String.valueOf((jdbcConfig.getConnectTimeOut()) * 1000));
        }
        JdbcUtil.putExtParam(jdbcConfig, properties);
    }
}
