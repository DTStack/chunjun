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

package com.dtstack.chunjun.connector.sybase.source;

import com.dtstack.chunjun.config.SyncConf;
import com.dtstack.chunjun.connector.jdbc.source.JdbcSourceFactory;
import com.dtstack.chunjun.connector.jdbc.util.JdbcUtil;
import com.dtstack.chunjun.connector.sybase.dialect.SybaseDialect;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.commons.lang3.StringUtils;

/** @Author OT @Date 2022/6/13 17:42 @Version 1.0 */
public class SybaseSourceFactory extends JdbcSourceFactory {
    public SybaseSourceFactory(SyncConf syncConf, StreamExecutionEnvironment env) {
        super(syncConf, env, new SybaseDialect());
        // avoid result.next blocking
        if (jdbcConf.isPolling()
                && StringUtils.isEmpty(jdbcConf.getStartLocation())
                && jdbcConf.getFetchSize() == 0) {
            jdbcConf.setFetchSize(1000);
        }
        JdbcUtil.putExtParam(jdbcConf);
    }
}
