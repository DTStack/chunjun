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
package com.dtstack.chunjun.connector.oraclelogminer.table;

import com.dtstack.chunjun.connector.oraclelogminer.config.LogMinerConfig;
import com.dtstack.chunjun.connector.oraclelogminer.options.LogminerOptions;
import com.dtstack.chunjun.connector.oraclelogminer.source.OraclelogminerDynamicTableSource;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.HashSet;
import java.util.Set;

public class OraclelogminerDynamicTableFactory implements DynamicTableSourceFactory {
    public static final String IDENTIFIER = "oraclelogminer-x";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(LogminerOptions.JDBC_URL);
        options.add(LogminerOptions.USERNAME);
        options.add(LogminerOptions.PASSWORD);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(LogminerOptions.FETCHSIZE);
        options.add(LogminerOptions.CAT);
        options.add(LogminerOptions.POSITION);
        options.add(LogminerOptions.START_TIME);
        options.add(LogminerOptions.START_SCN);
        options.add(LogminerOptions.TABLE);
        options.add(LogminerOptions.QUERY_TIMEOUT);
        options.add(LogminerOptions.SUPPORT_AUTO_LOG);
        options.add(LogminerOptions.IO_THREADS);
        options.add(LogminerOptions.MAX_LOAD_FILE_SIZE);
        options.add(LogminerOptions.TRANSACTION_CACHE_NUM_SIZE);
        options.add(LogminerOptions.TRANSACTION_EXPIRE_TIME);
        options.add(LogminerOptions.TIMESTAMP_FORMAT);
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
        ResolvedSchema resolvedSchema = context.getCatalogTable().getResolvedSchema();
        LogMinerConfig logMinerConfig = getLogMinerConf(config);
        return new OraclelogminerDynamicTableSource(
                resolvedSchema, logMinerConfig, LogminerOptions.getTimestampFormat(config));
    }

    /**
     * 初始化LogMinerConf
     *
     * @param config LogMinerConf
     * @return
     */
    private LogMinerConfig getLogMinerConf(ReadableConfig config) {
        LogMinerConfig logMinerConfig = new LogMinerConfig();
        logMinerConfig.setUsername(config.get(LogminerOptions.USERNAME));
        logMinerConfig.setPassword(config.get(LogminerOptions.PASSWORD));
        logMinerConfig.setJdbcUrl(config.get(LogminerOptions.JDBC_URL));

        logMinerConfig.setReadPosition(config.get(LogminerOptions.POSITION));
        logMinerConfig.setStartTime(config.get(LogminerOptions.START_TIME));
        logMinerConfig.setStartScn(config.get(LogminerOptions.START_SCN));

        logMinerConfig.setListenerTables(config.get(LogminerOptions.TABLE));
        logMinerConfig.setCat(config.get(LogminerOptions.CAT));

        logMinerConfig.setFetchSize(config.get(LogminerOptions.FETCHSIZE));
        logMinerConfig.setQueryTimeout(config.get(LogminerOptions.QUERY_TIMEOUT));
        logMinerConfig.setSupportAutoAddLog(config.get(LogminerOptions.SUPPORT_AUTO_LOG));
        logMinerConfig.setMaxLogFileSize(config.get(LogminerOptions.MAX_LOAD_FILE_SIZE));

        logMinerConfig.setIoThreads(config.get(LogminerOptions.IO_THREADS));

        logMinerConfig.setTransactionCacheNumSize(
                config.get(LogminerOptions.TRANSACTION_CACHE_NUM_SIZE));
        logMinerConfig.setTransactionExpireTime(
                config.get(LogminerOptions.TRANSACTION_EXPIRE_TIME));

        logMinerConfig.setPavingData(true);
        logMinerConfig.setSplit(true);

        return logMinerConfig;
    }
}
