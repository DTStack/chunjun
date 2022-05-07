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

package com.dtstack.flinkx.connector.mysql.table;

import com.dtstack.flinkx.connector.jdbc.conf.JdbcConf;
import com.dtstack.flinkx.connector.jdbc.dialect.JdbcDialect;
import com.dtstack.flinkx.connector.jdbc.sink.JdbcOutputFormatBuilder;
import com.dtstack.flinkx.connector.jdbc.source.JdbcInputFormatBuilder;
import com.dtstack.flinkx.connector.jdbc.table.JdbcDynamicTableFactory;
import com.dtstack.flinkx.connector.jdbc.util.JdbcUtil;
import com.dtstack.flinkx.connector.mysql.dialect.MysqlDialect;
import com.dtstack.flinkx.connector.mysql.sink.MysqlOutputFormat;
import com.dtstack.flinkx.connector.mysql.source.MysqlInputFormat;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;

/**
 * @program: flinkx
 * @author: wuren
 * @create: 2021/03/17
 */
public class MysqlDynamicTableFactory extends JdbcDynamicTableFactory {

    // 默认是Mysql流式拉取
    private static final int DEFAULT_FETCH_SIZE = Integer.MIN_VALUE;

    /** 通过该值查找具体插件 */
    private static final String IDENTIFIER = "mysql-x";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    protected JdbcDialect getDialect() {
        return new MysqlDialect();
    }

    @Override
    protected int getDefaultFetchSize() {
        return DEFAULT_FETCH_SIZE;
    }

    @Override
    protected JdbcConf getSourceConnectionConf(ReadableConfig readableConfig) {
        JdbcConf jdbcConf = super.getSourceConnectionConf(readableConfig);
        JdbcUtil.putExtParam(jdbcConf);
        return jdbcConf;
    }

    @Override
    protected JdbcConf getSinkConnectionConf(ReadableConfig readableConfig, TableSchema schema) {
        JdbcConf jdbcConf = super.getSinkConnectionConf(readableConfig, schema);
        JdbcUtil.putExtParam(jdbcConf);
        return jdbcConf;
    }

    /**
     * 获取JDBC插件的具体inputFormatBuilder
     *
     * @return JdbcInputFormatBuilder
     */
    @Override
    protected JdbcInputFormatBuilder getInputFormatBuilder() {
        return new JdbcInputFormatBuilder(new MysqlInputFormat());
    }

    /**
     * 获取JDBC插件的具体outputFormatBuilder
     *
     * @return JdbcOutputFormatBuilder
     */
    @Override
    protected JdbcOutputFormatBuilder getOutputFormatBuilder() {
        return new JdbcOutputFormatBuilder(new MysqlOutputFormat());
    }
}
