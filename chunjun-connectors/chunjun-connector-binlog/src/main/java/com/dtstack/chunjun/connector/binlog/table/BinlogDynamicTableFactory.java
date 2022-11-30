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
package com.dtstack.chunjun.connector.binlog.table;

import com.dtstack.chunjun.connector.binlog.config.BinlogConfig;
import com.dtstack.chunjun.connector.binlog.options.BinlogOptions;
import com.dtstack.chunjun.connector.binlog.source.BinlogDynamicTableSource;
import com.dtstack.chunjun.constants.ConstantValue;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public class BinlogDynamicTableFactory implements DynamicTableSourceFactory {
    public static final String IDENTIFIER = "binlog-x";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(BinlogOptions.JDBC_URL);
        options.add(BinlogOptions.USERNAME);
        options.add(BinlogOptions.PASSWORD);
        options.add(BinlogOptions.HOST);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(BinlogOptions.PORT);
        options.add(BinlogOptions.TABLE);
        options.add(BinlogOptions.FILTER);
        options.add(BinlogOptions.CAT);
        options.add(BinlogOptions.JOURNAL_NAME);
        options.add(BinlogOptions.TIMESTAMP);
        options.add(BinlogOptions.POSITION);
        options.add(BinlogOptions.CONNECTION_CHARSET);
        options.add(BinlogOptions.DETECTING_ENABLE);
        options.add(BinlogOptions.DETECTING_SQL);
        options.add(BinlogOptions.ENABLE_TSDB);
        options.add(BinlogOptions.PERIOD);
        options.add(BinlogOptions.BUFFER_SIZE);
        options.add(BinlogOptions.PARALLEL);
        options.add(BinlogOptions.PARALLEL_THREAD_SIZE);
        options.add(BinlogOptions.IS_GTID_MODE);
        options.add(BinlogOptions.QUERY_TIME_OUT);
        options.add(BinlogOptions.CONNECT_TIME_OUT);
        options.add(BinlogOptions.TIMESTAMP_FORMAT);
        return options;
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        // 1.所有的requiredOptions和optionalOptions参数
        final ReadableConfig config = helper.getOptions();

        // 2.参数校验
        helper.validate();

        // 3.封装参数
        ResolvedSchema schema = context.getCatalogTable().getResolvedSchema();
        DataType dataType = context.getPhysicalRowDataType();

        BinlogConfig binlogConfig = getBinlogConf(config);

        return new BinlogDynamicTableSource(
                schema, binlogConfig, BinlogOptions.getTimestampFormat(config), dataType);
    }

    /**
     * 初始化BinlogConfig
     *
     * @param config BinlogConfig
     * @return binlog config
     */
    private BinlogConfig getBinlogConf(ReadableConfig config) {
        BinlogConfig binlogConfig = new BinlogConfig();
        binlogConfig.setHost(config.get(BinlogOptions.HOST));
        binlogConfig.setPort(config.get(BinlogOptions.PORT));
        binlogConfig.setUsername(config.get(BinlogOptions.USERNAME));
        binlogConfig.setPassword(config.get(BinlogOptions.PASSWORD));
        binlogConfig.setJdbcUrl(config.get(BinlogOptions.JDBC_URL));

        HashMap<String, Object> start = new HashMap<>();
        String journalName = config.get(BinlogOptions.JOURNAL_NAME);
        Long timestamp = config.get(BinlogOptions.TIMESTAMP);
        Long position = config.get(BinlogOptions.POSITION);
        start.put(BinlogOptions.JOURNAL_NAME.key(), journalName);
        start.put(BinlogOptions.TIMESTAMP.key(), timestamp);
        start.put(BinlogOptions.POSITION.key(), position);
        binlogConfig.setStart(start);

        binlogConfig.setCat(config.get(BinlogOptions.CAT));
        binlogConfig.setFilter(config.get(BinlogOptions.FILTER));
        binlogConfig.setPeriod(config.get(BinlogOptions.PERIOD));
        binlogConfig.setBufferSize(config.get(BinlogOptions.BUFFER_SIZE));
        binlogConfig.setPavingData(true);
        binlogConfig.setTable(
                Arrays.asList(config.get(BinlogOptions.TABLE).split(ConstantValue.COMMA_SYMBOL)));
        binlogConfig.setConnectionCharset(config.get(BinlogOptions.CONNECTION_CHARSET));
        binlogConfig.setDetectingEnable(config.get(BinlogOptions.DETECTING_ENABLE));
        binlogConfig.setDetectingSQL(config.get(BinlogOptions.DETECTING_SQL));
        binlogConfig.setEnableTsdb(config.get(BinlogOptions.ENABLE_TSDB));
        binlogConfig.setParallel(config.get(BinlogOptions.PARALLEL));
        binlogConfig.setParallelThreadSize(config.get(BinlogOptions.PARALLEL_THREAD_SIZE));
        binlogConfig.setGTIDMode(config.get(BinlogOptions.IS_GTID_MODE));
        binlogConfig.setSplit(true);
        binlogConfig.setQueryTimeOut(config.get(BinlogOptions.QUERY_TIME_OUT));
        binlogConfig.setConnectTimeOut(config.get(BinlogOptions.CONNECT_TIME_OUT));

        return binlogConfig;
    }
}
