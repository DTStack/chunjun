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

import com.dtstack.chunjun.connector.sqlservercdc.conf.SqlServerCdcConf;
import com.dtstack.chunjun.connector.sqlservercdc.options.SqlServerCdcOptions;
import com.dtstack.chunjun.connector.sqlservercdc.source.SqlServerCdcDynamicTableSource;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.formats.json.JsonOptions;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.utils.TableSchemaUtils;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

@SuppressWarnings("all")
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
        options.add(JsonOptions.TIMESTAMP_FORMAT);
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
        TableSchema physicalSchema =
                TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());
        SqlServerCdcConf serverCdcConf = getSqlServerCdcConf(config);

        return new SqlServerCdcDynamicTableSource(
                physicalSchema, serverCdcConf, JsonOptions.getTimestampFormat(config));
    }

    /**
     * 初始化BinlogConf
     *
     * @param config BinlogConf
     * @return
     */
    private SqlServerCdcConf getSqlServerCdcConf(ReadableConfig config) {
        SqlServerCdcConf sqlServerCdcConf = new SqlServerCdcConf();
        sqlServerCdcConf.setUsername(config.get(SqlServerCdcOptions.USERNAME));
        sqlServerCdcConf.setPassword(config.get(SqlServerCdcOptions.PASSWORD));
        sqlServerCdcConf.setUrl(config.get(SqlServerCdcOptions.JDBC_URL));
        sqlServerCdcConf.setPollInterval(config.get(SqlServerCdcOptions.POLLINTERVAL));
        sqlServerCdcConf.setCat(config.get(SqlServerCdcOptions.CAT));
        sqlServerCdcConf.setPavingData(true);
        sqlServerCdcConf.setDatabaseName(config.get(SqlServerCdcOptions.DATABASE));
        sqlServerCdcConf.setTableList(Arrays.asList(config.get(SqlServerCdcOptions.TABLE)));
        sqlServerCdcConf.setAutoCommit(config.get(SqlServerCdcOptions.AUTO_COMMIT));
        sqlServerCdcConf.setAutoResetConnection(
                config.get(SqlServerCdcOptions.AUTO_RESET_CONNECTION));

        return sqlServerCdcConf;
    }
}
