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
package com.dtstack.chunjun.connector.oceanbase.source;

import com.dtstack.chunjun.config.SyncConfig;
import com.dtstack.chunjun.connector.jdbc.config.JdbcConfig;
import com.dtstack.chunjun.connector.jdbc.source.JdbcSourceFactory;
import com.dtstack.chunjun.connector.oceanbase.config.OceanBaseConf;
import com.dtstack.chunjun.connector.oceanbase.config.OceanBaseMode;
import com.dtstack.chunjun.connector.oceanbase.dialect.OceanbaseMysqlModeDialect;
import com.dtstack.chunjun.connector.oceanbase.dialect.OceanbaseOracleModeDialect;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.commons.lang3.StringUtils;

import java.util.Properties;

public class OceanbaseSourceFactory extends JdbcSourceFactory {
    // 默认是Mysql流式拉取
    private static final int DEFAULT_FETCH_SIZE = Integer.MIN_VALUE;
    private static final String ORACLE_JDBC_READ_TIMEOUT = "oracle.jdbc.ReadTimeout";
    private static final String ORACLE_NET_CONNECT_TIMEOUT = "oracle.net.CONNECT_TIMEOUT";

    private OceanBaseConf oceanBaseConf;

    public OceanbaseSourceFactory(SyncConfig syncConfig, StreamExecutionEnvironment env) {
        super(syncConfig, env, new OceanbaseMysqlModeDialect()); // 默认为mysql方言
        this.oceanBaseConf = (OceanBaseConf) this.jdbcConfig;
        if (oceanBaseConf != null) {
            OceanBaseMode mode = OceanBaseMode.valueOf(oceanBaseConf.getOceanBaseMode());
            // 若是for oracle模式
            if (mode == OceanBaseMode.ORACLE) {
                // 设置for oracle方言
                this.jdbcDialect = new OceanbaseOracleModeDialect();
                Properties properties = jdbcConfig.getProperties();
                if (properties == null) {
                    properties = new Properties();
                }
                if (jdbcConfig.getConnectTimeOut() != 0) {
                    // queryTimeOut单位是秒 需要转换成毫秒
                    properties.putIfAbsent(
                            ORACLE_JDBC_READ_TIMEOUT,
                            String.valueOf(jdbcConfig.getQueryTimeOut() * 1000));
                    properties.putIfAbsent(
                            ORACLE_NET_CONNECT_TIMEOUT,
                            String.valueOf(jdbcConfig.getQueryTimeOut() * 3 * 1000));
                    jdbcConfig.setProperties(properties);
                }
            } else {
                // 其他情况：for mysql模式 初始化
                // 避免result.next阻塞
                if (jdbcConfig.isPolling()
                        && StringUtils.isEmpty(jdbcConfig.getStartLocation())
                        && jdbcConfig.getFetchSize() == 0) {
                    jdbcConfig.setFetchSize(1000);
                }
            }
        }
    }

    @Override
    protected Class<? extends JdbcConfig> getConfClass() {
        return OceanBaseConf.class;
    }

    @Override
    protected int getDefaultFetchSize() {
        if (oceanBaseConf != null) {
            OceanBaseMode mode = OceanBaseMode.valueOf(oceanBaseConf.getOceanBaseMode());
            // 处理for oracle情况
            if (mode == OceanBaseMode.ORACLE) {
                return super.getDefaultFetchSize();
            }
        }
        return DEFAULT_FETCH_SIZE;
    }
}
