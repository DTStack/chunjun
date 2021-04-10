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
import org.apache.flink.connector.jdbc.dialect.JdbcDialects;
import org.apache.flink.connector.jdbc.internal.options.JdbcDmlOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcReadOptions;
import org.apache.flink.connector.jdbc.table.JdbcDynamicTableSink;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.util.Preconditions;

import com.dtstack.flinkx.lookup.options.LookupOptions;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static com.dtstack.flinkx.connector.jdbc.constants.JdbcCommonConstants.PASSWORD;
import static com.dtstack.flinkx.connector.jdbc.constants.JdbcCommonConstants.TABLE_NAME;
import static com.dtstack.flinkx.connector.jdbc.constants.JdbcCommonConstants.URL;
import static com.dtstack.flinkx.connector.jdbc.constants.JdbcCommonConstants.USERNAME;
import static com.dtstack.flinkx.connector.jdbc.constants.JdbcSinkConstants.SINK_BUFFER_FLUSH_INTERVAL;
import static com.dtstack.flinkx.connector.jdbc.constants.JdbcSinkConstants.SINK_BUFFER_FLUSH_MAX_ROWS;
import static com.dtstack.flinkx.connector.jdbc.constants.JdbcSinkConstants.SINK_MAX_RETRIES;
import static com.dtstack.flinkx.connector.jdbc.constants.JdbcSourceConstants.SCAN_AUTO_COMMIT;
import static com.dtstack.flinkx.connector.jdbc.constants.JdbcSourceConstants.SCAN_FETCH_SIZE;
import static com.dtstack.flinkx.connector.jdbc.constants.JdbcSourceConstants.SCAN_PARTITION_COLUMN;
import static com.dtstack.flinkx.connector.jdbc.constants.JdbcSourceConstants.SCAN_PARTITION_LOWER_BOUND;
import static com.dtstack.flinkx.connector.jdbc.constants.JdbcSourceConstants.SCAN_PARTITION_NUM;
import static com.dtstack.flinkx.connector.jdbc.constants.JdbcSourceConstants.SCAN_PARTITION_UPPER_BOUND;
import static com.dtstack.flinkx.lookup.constants.LookUpConstants.LOOKUP_CACHE_MAX_ROWS;
import static com.dtstack.flinkx.lookup.constants.LookUpConstants.LOOKUP_CACHE_PERIOD;
import static com.dtstack.flinkx.lookup.constants.LookUpConstants.LOOKUP_CACHE_TTL;
import static com.dtstack.flinkx.lookup.constants.LookUpConstants.LOOKUP_CACHE_TYPE;
import static com.dtstack.flinkx.lookup.constants.LookUpConstants.LOOKUP_FETCH_SIZE;
import static com.dtstack.flinkx.lookup.constants.LookUpConstants.LOOKUP_MAX_RETRIES;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * @author chuixue
 * @create 2021-04-10 12:54
 * @description
 **/
public abstract class JdbcDynamicTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {

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

    protected JdbcOptions getJdbcOptions(ReadableConfig readableConfig) {
        final String url = readableConfig.get(URL);
        final JdbcOptions.Builder builder =
                JdbcOptions.builder()
                        .setDBUrl(url)
                        .setTableName(readableConfig.get(TABLE_NAME))
                        .setDialect(getDialect());

        getDriver().ifPresent(builder::setDriverName);
        readableConfig.getOptional(USERNAME).ifPresent(builder::setUsername);
        readableConfig.getOptional(PASSWORD).ifPresent(builder::setPassword);
        return builder.build();
    }

    protected JdbcReadOptions getJdbcReadOptions(ReadableConfig readableConfig) {
        final Optional<String> partitionColumnName =
                readableConfig.getOptional(SCAN_PARTITION_COLUMN);
        final JdbcReadOptions.Builder builder = JdbcReadOptions.builder();
        if (partitionColumnName.isPresent()) {
            builder.setPartitionColumnName(partitionColumnName.get());
            builder.setPartitionLowerBound(readableConfig.get(SCAN_PARTITION_LOWER_BOUND));
            builder.setPartitionUpperBound(readableConfig.get(SCAN_PARTITION_UPPER_BOUND));
            builder.setNumPartitions(readableConfig.get(SCAN_PARTITION_NUM));
        }
        readableConfig.getOptional(SCAN_FETCH_SIZE).ifPresent(builder::setFetchSize);
        builder.setAutoCommit(readableConfig.get(SCAN_AUTO_COMMIT));
        return builder.build();
    }

    protected LookupOptions getJdbcLookupOptions(ReadableConfig readableConfig, String tableName) {
        return LookupOptions
                .build()
                .setTableName(tableName)
                .setPeriod(readableConfig.get(LOOKUP_CACHE_PERIOD))
                .setCacheSize(readableConfig.get(LOOKUP_CACHE_MAX_ROWS))
                .setCacheTtl(readableConfig.get(LOOKUP_CACHE_TTL))
                .setCache(readableConfig.get(LOOKUP_CACHE_TYPE))
                .setMaxRetryTimes(readableConfig.get(LOOKUP_MAX_RETRIES))
                .setFetchSize(readableConfig.get(LOOKUP_FETCH_SIZE));
    }

    private JdbcExecutionOptions getJdbcExecutionOptions(ReadableConfig config) {
        final JdbcExecutionOptions.Builder builder = new JdbcExecutionOptions.Builder();
        builder.withBatchSize(config.get(SINK_BUFFER_FLUSH_MAX_ROWS));
        builder.withBatchIntervalMs(config.get(SINK_BUFFER_FLUSH_INTERVAL).toMillis());
        builder.withMaxRetries(config.get(SINK_MAX_RETRIES));
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

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> requiredOptions = new HashSet<>();
        requiredOptions.add(URL);
        requiredOptions.add(TABLE_NAME);
        return requiredOptions;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> optionalOptions = new HashSet<>();
        optionalOptions.add(USERNAME);
        optionalOptions.add(PASSWORD);
        optionalOptions.add(SCAN_PARTITION_COLUMN);
        optionalOptions.add(SCAN_PARTITION_LOWER_BOUND);
        optionalOptions.add(SCAN_PARTITION_UPPER_BOUND);
        optionalOptions.add(SCAN_PARTITION_NUM);
        optionalOptions.add(SCAN_FETCH_SIZE);
        optionalOptions.add(SCAN_AUTO_COMMIT);
        optionalOptions.add(LOOKUP_CACHE_MAX_ROWS);
        optionalOptions.add(LOOKUP_CACHE_TTL);
        optionalOptions.add(LOOKUP_MAX_RETRIES);
        optionalOptions.add(LOOKUP_CACHE_TYPE);
        optionalOptions.add(SINK_BUFFER_FLUSH_MAX_ROWS);
        optionalOptions.add(SINK_BUFFER_FLUSH_INTERVAL);
        optionalOptions.add(SINK_MAX_RETRIES);
        return optionalOptions;
    }

    protected void validateConfigOptions(ReadableConfig config) {
        String jdbcUrl = config.get(URL);
        final Optional<JdbcDialect> dialect = JdbcDialects.get(jdbcUrl);
        checkState(dialect.isPresent(), "Cannot handle such jdbc url: " + jdbcUrl);

        checkAllOrNone(config, new ConfigOption[]{USERNAME, PASSWORD});

        checkAllOrNone(
                config,
                new ConfigOption[]{
                        SCAN_PARTITION_COLUMN,
                        SCAN_PARTITION_NUM,
                        SCAN_PARTITION_LOWER_BOUND,
                        SCAN_PARTITION_UPPER_BOUND
                });

        if (config.getOptional(SCAN_PARTITION_LOWER_BOUND).isPresent()
                && config.getOptional(SCAN_PARTITION_UPPER_BOUND).isPresent()) {
            long lowerBound = config.get(SCAN_PARTITION_LOWER_BOUND);
            long upperBound = config.get(SCAN_PARTITION_UPPER_BOUND);
            if (lowerBound > upperBound) {
                throw new IllegalArgumentException(
                        String.format(
                                "'%s'='%s' must not be larger than '%s'='%s'.",
                                SCAN_PARTITION_LOWER_BOUND.key(),
                                lowerBound,
                                SCAN_PARTITION_UPPER_BOUND.key(),
                                upperBound));
            }
        }

        checkAllOrNone(config, new ConfigOption[]{LOOKUP_CACHE_MAX_ROWS, LOOKUP_CACHE_TTL});

        if (config.get(LOOKUP_MAX_RETRIES) < 0) {
            throw new IllegalArgumentException(
                    String.format(
                            "The value of '%s' option shouldn't be negative, but is %s.",
                            LOOKUP_MAX_RETRIES.key(), config.get(LOOKUP_MAX_RETRIES)));
        }

        if (config.get(SINK_MAX_RETRIES) < 0) {
            throw new IllegalArgumentException(
                    String.format(
                            "The value of '%s' option shouldn't be negative, but is %s.",
                            SINK_MAX_RETRIES.key(), config.get(SINK_MAX_RETRIES)));
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

    /**
     * 子类根据不同数据库定义不同标记
     *
     * @return
     */
    @Override
    public abstract String factoryIdentifier();

    /**
     * 不同数据库不同方言
     *
     * @return
     */
    protected abstract JdbcDialect getDialect();

    /**
     * 不同数据库不同驱动
     *
     * @return
     */
    protected abstract Optional<String> getDriver();
}
