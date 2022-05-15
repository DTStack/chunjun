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

import com.dtstack.chunjun.connector.binlog.conf.BinlogConf;
import com.dtstack.chunjun.connector.binlog.options.BinlogOptions;
import com.dtstack.chunjun.connector.binlog.source.BinlogDynamicTableSource;
import com.dtstack.chunjun.constants.ConstantValue;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.formats.json.JsonOptions;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.utils.TableSchemaUtils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

/**
 * Date: 2021/04/27 Company: www.dtstack.com
 *
 * @author tudou
 */
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
        options.add(JsonOptions.TIMESTAMP_FORMAT);
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
        TableSchema physicalSchema =
                TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());
        BinlogConf binlogConf = getBinlogConf(config);

        return new BinlogDynamicTableSource(
                physicalSchema, binlogConf, JsonOptions.getTimestampFormat(config));
    }

    /**
     * 初始化BinlogConf
     *
     * @param config BinlogConf
     * @return
     */
    private BinlogConf getBinlogConf(ReadableConfig config) {
        BinlogConf binlogConf = new BinlogConf();
        binlogConf.setHost(config.get(BinlogOptions.HOST));
        binlogConf.setPort(config.get(BinlogOptions.PORT));
        binlogConf.setUsername(config.get(BinlogOptions.USERNAME));
        binlogConf.setPassword(config.get(BinlogOptions.PASSWORD));
        binlogConf.setJdbcUrl(config.get(BinlogOptions.JDBC_URL));

        HashMap<String, Object> start = new HashMap<>();
        String journalName = config.get(BinlogOptions.JOURNAL_NAME);
        Long timestamp = config.get(BinlogOptions.TIMESTAMP);
        Long position = config.get(BinlogOptions.POSITION);
        start.put(BinlogOptions.JOURNAL_NAME.key(), journalName);
        start.put(BinlogOptions.TIMESTAMP.key(), timestamp);
        start.put(BinlogOptions.POSITION.key(), position);
        binlogConf.setStart(start);

        binlogConf.setCat(config.get(BinlogOptions.CAT));
        binlogConf.setFilter(config.get(BinlogOptions.FILTER));
        binlogConf.setPeriod(config.get(BinlogOptions.PERIOD));
        binlogConf.setBufferSize(config.get(BinlogOptions.BUFFER_SIZE));
        binlogConf.setPavingData(true);
        binlogConf.setTable(
                Arrays.asList(config.get(BinlogOptions.TABLE).split(ConstantValue.COMMA_SYMBOL)));
        binlogConf.setConnectionCharset(config.get(BinlogOptions.CONNECTION_CHARSET));
        binlogConf.setDetectingEnable(config.get(BinlogOptions.DETECTING_ENABLE));
        binlogConf.setDetectingSQL(config.get(BinlogOptions.DETECTING_SQL));
        binlogConf.setEnableTsdb(config.get(BinlogOptions.ENABLE_TSDB));
        binlogConf.setParallel(config.get(BinlogOptions.PARALLEL));
        binlogConf.setParallelThreadSize(config.get(BinlogOptions.PARALLEL_THREAD_SIZE));
        binlogConf.setGTIDMode(config.get(BinlogOptions.IS_GTID_MODE));
        binlogConf.setSplit(true);
        binlogConf.setQueryTimeOut(config.get(BinlogOptions.QUERY_TIME_OUT));
        binlogConf.setConnectTimeOut(config.get(BinlogOptions.CONNECT_TIME_OUT));

        return binlogConf;
    }
}
