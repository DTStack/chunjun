package com.dtstack.chunjun.connector.db2.source;
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

import com.dtstack.chunjun.conf.SyncConf;
import com.dtstack.chunjun.connector.db2.dialect.Db2Dialect;
import com.dtstack.chunjun.connector.jdbc.source.JdbcInputFormatBuilder;
import com.dtstack.chunjun.connector.jdbc.source.JdbcSourceFactory;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.commons.lang3.StringUtils;

/**
 * Company: www.dtstack.com
 *
 * @author xuchao
 * @date 2021-06-15
 */
public class Db2SourceFactory extends JdbcSourceFactory {
    public Db2SourceFactory(SyncConf syncConf, StreamExecutionEnvironment env) {
        super(syncConf, env, new Db2Dialect());
        // 避免result.next阻塞
        if (jdbcConf.isPolling()
                && StringUtils.isEmpty(jdbcConf.getStartLocation())
                && jdbcConf.getFetchSize() == 0) {
            jdbcConf.setFetchSize(1000);
        }
    }

    @Override
    protected JdbcInputFormatBuilder getBuilder() {
        return new JdbcInputFormatBuilder(new Db2InputFormat());
    }
}
