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

package com.dtstack.flinkx.connector.inceptor.source;

import com.dtstack.flinkx.conf.SyncConf;
import com.dtstack.flinkx.connector.inceptor.conf.InceptorConf;
import com.dtstack.flinkx.connector.inceptor.dialect.InceptorDialect;
import com.dtstack.flinkx.connector.inceptor.util.InceptorDbUtil;
import com.dtstack.flinkx.connector.jdbc.conf.JdbcConf;
import com.dtstack.flinkx.connector.jdbc.source.JdbcInputFormatBuilder;
import com.dtstack.flinkx.connector.jdbc.source.JdbcSourceFactory;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/** @author liuliu 2022/2/22 */
public class InceptorSourceFactory extends JdbcSourceFactory {

    InceptorDialect inceptorDialect;

    public InceptorSourceFactory(SyncConf syncConf, StreamExecutionEnvironment env) {
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
