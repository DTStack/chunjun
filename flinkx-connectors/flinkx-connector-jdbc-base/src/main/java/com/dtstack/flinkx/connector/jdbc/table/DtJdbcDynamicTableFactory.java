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

package com.dtstack.flinkx.connector.jdbc.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc.internal.options.JdbcDmlOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcOptions;
import org.apache.flink.connector.jdbc.table.JdbcDynamicTableFactory;
import org.apache.flink.connector.jdbc.table.JdbcDynamicTableSink;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.util.Preconditions;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * @program: flinkx
 * @author: wuren
 * @create: 2021/03/17
 **/
public abstract class DtJdbcDynamicTableFactory extends JdbcDynamicTableFactory {

    /**
     * 子类根据不同数据库定义不同标记
     *
     * @return
     */
    @Override
    public abstract String factoryIdentifier();

    /**
     * 必填选项
     *
     * @return
     */
    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> requiredOptions = new HashSet<>();
        requiredOptions.add(JdbcDdlOptions.URL);
        requiredOptions.add(JdbcDdlOptions.TABLE_NAME);
        return requiredOptions;
    }

    /**
     * 可选选项
     *
     * @return
     */
    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> optionalOptions = new HashSet<>();
        optionalOptions.add(JdbcDdlOptions.USERNAME);
        optionalOptions.add(JdbcDdlOptions.PASSWORD);
        optionalOptions.add(JdbcDdlOptions.SINK_BUFFER_FLUSH_MAX_ROWS);
        optionalOptions.add(JdbcDdlOptions.SINK_BUFFER_FLUSH_INTERVAL);
        optionalOptions.add(JdbcDdlOptions.SINK_MAX_RETRIES);
        return optionalOptions;
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        final ReadableConfig config = helper.getOptions();

        helper.validate();
        validateConfigOptions(config);
        JdbcOptions jdbcOptions = getJdbcOptions(config);
        TableSchema physicalSchema =
                TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());

        return new JdbcDynamicTableSink(
                jdbcOptions,
                getJdbcExecutionOptions(config),
                getJdbcDmlOptions(jdbcOptions, physicalSchema),
                physicalSchema);

    }

    private JdbcOptions getJdbcOptions(ReadableConfig readableConfig) {
        final String url = readableConfig.get(JdbcDdlOptions.URL);
        final JdbcOptions.Builder builder =
                JdbcOptions.builder()
                        .setDBUrl(url)
                        .setTableName(readableConfig.get(JdbcDdlOptions.TABLE_NAME))
                        .setDialect(getDialect());

        builder.setDriverName(getDriver());
        readableConfig.getOptional(JdbcDdlOptions.USERNAME).ifPresent(builder::setUsername);
        readableConfig.getOptional(JdbcDdlOptions.PASSWORD).ifPresent(builder::setPassword);
        return builder.build();
    }

    private JdbcExecutionOptions getJdbcExecutionOptions(ReadableConfig config) {
        final JdbcExecutionOptions.Builder builder = new JdbcExecutionOptions.Builder();
        builder.withBatchSize(config.get(JdbcDdlOptions.SINK_BUFFER_FLUSH_MAX_ROWS));
        builder.withBatchIntervalMs(config.get(JdbcDdlOptions.SINK_BUFFER_FLUSH_INTERVAL).toMillis());
        builder.withMaxRetries(config.get(JdbcDdlOptions.SINK_MAX_RETRIES));
        return builder.build();
    }

    private JdbcDmlOptions getJdbcDmlOptions(JdbcOptions jdbcOptions, TableSchema schema) {
        String[] keyFields =
                schema.getPrimaryKey()
                        .map(pk -> pk.getColumns().toArray(new String[0]))
                        .orElse(null);

        return JdbcDmlOptions.builder()
                .withTableName(jdbcOptions.getTableName())
                .withDialect(jdbcOptions.getDialect())
                .withFieldNames(schema.getFieldNames())
                .withKeyFields(keyFields)
                .build();
    }

    private void validateConfigOptions(ReadableConfig config) {
        checkAllOrNone(config, new ConfigOption[] {USERNAME, PASSWORD});

        if (config.get(JdbcDdlOptions.SINK_MAX_RETRIES) < 0) {
            throw new IllegalArgumentException(
                    String.format(
                            "The value of '%s' option shouldn't be negative, but is %s.",
                            JdbcDdlOptions.SINK_MAX_RETRIES.key(), config.get(JdbcDdlOptions.SINK_MAX_RETRIES)));
        }
    }

    private void checkAllOrNone(ReadableConfig config, ConfigOption<?>[] configOptions) {
        int presentCount = 0;
        for (ConfigOption configOption : configOptions) {
            if (config.getOptional(configOption).isPresent()) {
                presentCount++;
            }
        }
        String[] propertyNames =
                Arrays.stream(configOptions).map(ConfigOption::key).toArray(String[]::new);
        Preconditions.checkArgument(
                configOptions.length == presentCount || presentCount == 0,
                "Either all or none of the following options should be provided:\n"
                        + String.join("\n", propertyNames));
    }

    protected abstract JdbcDialect getDialect();

    // TODO Driver 是否可以用户传入或者自动适配
    protected abstract String getDriver();

}