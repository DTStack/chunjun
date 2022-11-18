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

package com.dtstack.chunjun.connector.oceanbasecdc.table;

import com.dtstack.chunjun.connector.oceanbasecdc.config.OceanBaseCdcConfig;
import com.dtstack.chunjun.connector.oceanbasecdc.options.OceanBaseCdcOptions;
import com.dtstack.chunjun.connector.oceanbasecdc.source.OceanBaseCdcDynamicTableSource;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;

import com.oceanbase.clogproxy.client.config.ObReaderConfig;
import org.apache.commons.lang3.StringUtils;

import java.util.HashSet;
import java.util.Set;

public class OceanBaseCdcDynamicTableFactory implements DynamicTableSourceFactory {

    public static final String IDENTIFIER = "oceanbasecdc-x";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(OceanBaseCdcOptions.LOG_PROXY_HOST);
        options.add(OceanBaseCdcOptions.LOG_PROXY_PORT);
        options.add(OceanBaseCdcOptions.USERNAME);
        options.add(OceanBaseCdcOptions.PASSWORD);
        options.add(OceanBaseCdcOptions.TABLE_WHITELIST);
        options.add(OceanBaseCdcOptions.STARTUP_TIMESTAMP);

        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(OceanBaseCdcOptions.RS_LIST);
        options.add(OceanBaseCdcOptions.CONFIG_URL);
        options.add(OceanBaseCdcOptions.TIMEZONE);
        options.add(OceanBaseCdcOptions.WORKING_MODE);
        options.add(OceanBaseCdcOptions.CAT);
        options.add(OceanBaseCdcOptions.TIMESTAMP_FORMAT);
        return options;
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();
        ResolvedSchema resolvedSchema = context.getCatalogTable().getResolvedSchema();
        final ReadableConfig config = helper.getOptions();
        OceanBaseCdcConfig cdcConf = getOceanBaseCdcConf(config);
        return new OceanBaseCdcDynamicTableSource(
                resolvedSchema, cdcConf, OceanBaseCdcOptions.getTimestampFormat(config));
    }

    private OceanBaseCdcConfig getOceanBaseCdcConf(ReadableConfig config) {
        OceanBaseCdcConfig cdcConf = new OceanBaseCdcConfig();
        cdcConf.setLogProxyHost(config.get(OceanBaseCdcOptions.LOG_PROXY_HOST));
        cdcConf.setLogProxyPort(config.get(OceanBaseCdcOptions.LOG_PROXY_PORT));
        cdcConf.setCat(config.get(OceanBaseCdcOptions.CAT));

        ObReaderConfig obReaderConfig = new ObReaderConfig();
        obReaderConfig.setUsername(config.get(OceanBaseCdcOptions.USERNAME));
        obReaderConfig.setPassword(config.get(OceanBaseCdcOptions.PASSWORD));
        obReaderConfig.setTableWhiteList(config.get(OceanBaseCdcOptions.TABLE_WHITELIST));
        obReaderConfig.setStartTimestamp(config.get(OceanBaseCdcOptions.STARTUP_TIMESTAMP));
        obReaderConfig.setTimezone(config.get(OceanBaseCdcOptions.TIMEZONE));
        obReaderConfig.setWorkingMode(config.get(OceanBaseCdcOptions.WORKING_MODE));
        String rsList = config.get(OceanBaseCdcOptions.RS_LIST);
        String configUrl = config.get(OceanBaseCdcOptions.CONFIG_URL);
        if (StringUtils.isNotEmpty(rsList)) {
            obReaderConfig.setRsList(rsList);
        }
        if (StringUtils.isNotEmpty(configUrl)) {
            obReaderConfig.setClusterUrl(configUrl);
        }
        cdcConf.setObReaderConfig(obReaderConfig);
        cdcConf.setTimestampFormat(config.get(OceanBaseCdcOptions.TIMESTAMP_FORMAT));
        return cdcConf;
    }
}
