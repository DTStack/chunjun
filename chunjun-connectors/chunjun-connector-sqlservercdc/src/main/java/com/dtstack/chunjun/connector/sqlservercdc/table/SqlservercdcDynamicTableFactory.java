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
package com.dtstack.chunjun.connector.sqlservercdc.table;

import com.dtstack.chunjun.connector.sqlservercdc.config.SqlServerCdcConfig;
import com.dtstack.chunjun.connector.sqlservercdc.options.SqlServerCdcOptions;
import com.dtstack.chunjun.connector.sqlservercdc.source.SqlServerCdcDynamicTableSource;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class SqlservercdcDynamicTableFactory implements DynamicTableSourceFactory {
    public static final String IDENTIFIER = "sqlservercdc-x";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(SqlServerCdcOptions.JDBC_URL);
        options.add(SqlServerCdcOptions.USERNAME);
        options.add(SqlServerCdcOptions.PASSWORD);
        options.add(SqlServerCdcOptions.DATABASE);
        options.add(SqlServerCdcOptions.TABLE);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(SqlServerCdcOptions.CAT);
        options.add(SqlServerCdcOptions.LSN);
        options.add(SqlServerCdcOptions.POLLINTERVAL);
        options.add(SqlServerCdcOptions.TIMESTAMP_FORMAT);
        return options;
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        // 1.all params includes requiredOptions and optionalOptions
        final ReadableConfig config = helper.getOptions();

        // 2.param validate
        helper.validate();

        // 3.
        ResolvedSchema resolvedSchema = context.getCatalogTable().getResolvedSchema();
        SqlServerCdcConfig serverCdcConf = getSqlServerCdcConf(config);

        return new SqlServerCdcDynamicTableSource(
                resolvedSchema, serverCdcConf, SqlServerCdcOptions.getTimestampFormat(config));
    }

    /**
     * 初始化BinlogConf
     *
     * @param config BinlogConf
     * @return
     */
    private SqlServerCdcConfig getSqlServerCdcConf(ReadableConfig config) {
        SqlServerCdcConfig sqlServerCdcConfig = new SqlServerCdcConfig();
        sqlServerCdcConfig.setUsername(config.get(SqlServerCdcOptions.USERNAME));
        sqlServerCdcConfig.setPassword(config.get(SqlServerCdcOptions.PASSWORD));
        sqlServerCdcConfig.setUrl(config.get(SqlServerCdcOptions.JDBC_URL));
        sqlServerCdcConfig.setPollInterval(config.get(SqlServerCdcOptions.POLLINTERVAL));
        sqlServerCdcConfig.setCat(config.get(SqlServerCdcOptions.CAT));
        sqlServerCdcConfig.setPavingData(true);
        sqlServerCdcConfig.setDatabaseName(config.get(SqlServerCdcOptions.DATABASE));
        sqlServerCdcConfig.setTableList(Arrays.asList(config.get(SqlServerCdcOptions.TABLE)));
        sqlServerCdcConfig.setAutoCommit(config.get(SqlServerCdcOptions.AUTO_COMMIT));
        sqlServerCdcConfig.setAutoResetConnection(
                config.get(SqlServerCdcOptions.AUTO_RESET_CONNECTION));

        return sqlServerCdcConfig;
    }
}
