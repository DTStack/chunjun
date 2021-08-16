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

package com.dtstack.flinkx.connector.phoenix5.table;

import com.dtstack.flinkx.connector.jdbc.dialect.JdbcDialect;
import com.dtstack.flinkx.connector.jdbc.sink.JdbcOutputFormatBuilder;
import com.dtstack.flinkx.connector.jdbc.source.JdbcInputFormatBuilder;
import com.dtstack.flinkx.connector.jdbc.table.JdbcDynamicTableFactory;
import com.dtstack.flinkx.connector.phoenix5.Phoenix5Dialect;
import com.dtstack.flinkx.connector.phoenix5.sink.Phoenix5OutputFormat;
import com.dtstack.flinkx.connector.phoenix5.sink.Phoenix5OutputFormatBuilder;
import com.dtstack.flinkx.connector.phoenix5.source.Phoenix5InputFormat;
import com.dtstack.flinkx.connector.phoenix5.source.Phoenix5InputFormatBuilder;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.connector.source.DynamicTableSource;

import java.util.Set;

import static com.dtstack.flinkx.connector.phoenix5.conf.Phoenix5Options.READ_FROM_HBASE;

/**
 * @author wujuan
 * @version 1.0
 * @date 2021/7/9 16:01 星期五
 * @email wujuan@dtstack.com
 * @company www.dtstack.com
 */
public class Phoenix5DynamicTableFactory extends JdbcDynamicTableFactory {

    /** 通过该值查找具体插件 */
    private static final String IDENTIFIER = "phoenix5-x";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    protected JdbcDialect getDialect() {
        return new Phoenix5Dialect();
    }

    @Override
    protected JdbcInputFormatBuilder getInputFormatBuilder() {
        return new Phoenix5InputFormatBuilder(new Phoenix5InputFormat());
    }

    @Override
    protected JdbcOutputFormatBuilder getOutputFormatBuilder() {
        return new Phoenix5OutputFormatBuilder(new Phoenix5OutputFormat());
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> configOptions = super.optionalOptions();
        configOptions.add(READ_FROM_HBASE);
        return configOptions;
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        //  TODO sql support read from hbase.
        return super.createDynamicTableSource(context);
    }
}
