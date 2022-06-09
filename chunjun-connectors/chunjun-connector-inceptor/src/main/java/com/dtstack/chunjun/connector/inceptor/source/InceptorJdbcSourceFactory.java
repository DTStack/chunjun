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

package com.dtstack.chunjun.connector.inceptor.source;

import com.dtstack.chunjun.conf.SyncConf;
import com.dtstack.chunjun.connector.inceptor.conf.InceptorConf;
import com.dtstack.chunjun.connector.inceptor.dialect.InceptorDialect;
import com.dtstack.chunjun.connector.inceptor.util.InceptorDbUtil;
import com.dtstack.chunjun.connector.jdbc.conf.JdbcConf;
import com.dtstack.chunjun.connector.jdbc.source.JdbcInputFormatBuilder;
import com.dtstack.chunjun.connector.jdbc.source.JdbcSourceFactory;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class InceptorJdbcSourceFactory extends JdbcSourceFactory {
    InceptorDialect inceptorDialect;

    public InceptorJdbcSourceFactory(SyncConf syncConf, StreamExecutionEnvironment env) {
        super(syncConf, env, null);
        this.inceptorDialect = InceptorDbUtil.getDialectWithDriverType(jdbcConf);
        jdbcConf.setJdbcUrl(inceptorDialect.appendJdbcTransactionType(jdbcConf.getJdbcUrl()));
        super.jdbcDialect = inceptorDialect;
    }

    @Override
    protected Class<? extends JdbcConf> getConfClass() {
        return InceptorConf.class;
    }

    @Override
    protected JdbcInputFormatBuilder getBuilder() {
        return inceptorDialect.getInputFormatBuilder();
    }
}
