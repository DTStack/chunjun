/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dtstack.chunjun.connector.hive.source;

import com.dtstack.chunjun.config.SyncConfig;
import com.dtstack.chunjun.connector.hive.converter.HiveRawTypeMapper;
import com.dtstack.chunjun.connector.hive.dialect.HiveDialect;
import com.dtstack.chunjun.connector.jdbc.source.JdbcInputFormatBuilder;
import com.dtstack.chunjun.connector.jdbc.source.JdbcSourceFactory;
import com.dtstack.chunjun.converter.RawTypeMapper;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.commons.lang3.StringUtils;

public class HiveSourceFactory extends JdbcSourceFactory {

    // 默认是Mysql流式拉取
    private static final int DEFAULT_FETCH_SIZE = Integer.MIN_VALUE;

    public HiveSourceFactory(SyncConfig syncConfig, StreamExecutionEnvironment env) {
        super(syncConfig, env, new HiveDialect());
        // 避免result.next阻塞
        if (jdbcConfig.isPolling()
                && StringUtils.isEmpty(jdbcConfig.getStartLocation())
                && jdbcConfig.getFetchSize() == 0) {
            jdbcConfig.setFetchSize(1000);
        }
    }

    @Override
    protected JdbcInputFormatBuilder getBuilder() {
        return new JdbcInputFormatBuilder(new HiveInputFormat());
    }

    @Override
    protected int getDefaultFetchSize() {
        return DEFAULT_FETCH_SIZE;
    }

    @Override
    public RawTypeMapper getRawTypeMapper() {
        return HiveRawTypeMapper::apply;
    }
}
