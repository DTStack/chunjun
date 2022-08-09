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
package com.dtstack.chunjun.connector.pgwal.table;

import com.dtstack.chunjun.connector.api.PGCDCServiceProcessor;
import com.dtstack.chunjun.connector.pgwal.conf.PGWalConf;
import com.dtstack.chunjun.connector.pgwal.options.PGWalOptions;
import com.dtstack.chunjun.source.DtInputFormatSourceFunction;
import com.dtstack.chunjun.table.connector.source.ParallelSourceFunctionProvider;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.jdbc.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc.dialect.JdbcDialects;
import org.apache.flink.connector.jdbc.internal.options.JdbcLookupOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcReadOptions;
import org.apache.flink.connector.jdbc.table.JdbcDynamicTableFactory;
import org.apache.flink.connector.jdbc.table.JdbcDynamicTableSource;
import org.apache.flink.formats.json.JsonOptions;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.util.Preconditions;

import java.sql.SQLException;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkState;

/** */
public class PGWalDynamicTableFactory extends JdbcDynamicTableFactory {
    public static final String IDENTIFIER = "pgwal-x";

    private static final ConfigOption<String> DRIVER =
            ConfigOptions.key("driver")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "the class name of the JDBC driver to use to connect to this URL. "
                                    + "If not set, it will automatically be derived from the URL.");

    private static final ConfigOption<String> SCAN_PARTITION_COLUMN =
            ConfigOptions.key("scan.partition.column")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the column name used for partitioning the input.");
    private static final ConfigOption<Integer> SCAN_PARTITION_NUM =
            ConfigOptions.key("scan.partition.num")
                    .intType()
                    .noDefaultValue()
                    .withDescription("the number of partitions.");
    private static final ConfigOption<Long> SCAN_PARTITION_LOWER_BOUND =
            ConfigOptions.key("scan.partition.lower-bound")
                    .longType()
                    .noDefaultValue()
                    .withDescription("the smallest value of the first partition.");
    private static final ConfigOption<Long> SCAN_PARTITION_UPPER_BOUND =
            ConfigOptions.key("scan.partition.upper-bound")
                    .longType()
                    .noDefaultValue()
                    .withDescription("the largest value of the last partition.");
    private static final ConfigOption<Integer> SCAN_FETCH_SIZE =
            ConfigOptions.key("scan.fetch-size")
                    .intType()
                    .defaultValue(0)
                    .withDescription(
                            "gives the reader a hint as to the number of rows that should be fetched, from"
                                    + " the database when reading per round trip. If the value specified is zero, then the hint is ignored. The"
                                    + " default value is zero.");
    private static final ConfigOption<Boolean> SCAN_AUTO_COMMIT =
            ConfigOptions.key("scan.auto-commit")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "sets whether the driver is in auto-commit mode. The default value is true, per"
                                    + " the JDBC spec.");

    // look up config options
    private static final ConfigOption<Long> LOOKUP_CACHE_MAX_ROWS =
            ConfigOptions.key("lookup.cache.max-rows")
                    .longType()
                    .defaultValue(-1L)
                    .withDescription(
                            "the max number of rows of lookup cache, over this value, the oldest rows will "
                                    + "be eliminated. \"cache.max-rows\" and \"cache.ttl\" options must all be specified if any of them is "
                                    + "specified. Cache is not enabled as default.");
    private static final ConfigOption<Duration> LOOKUP_CACHE_TTL =
            ConfigOptions.key("lookup.cache.ttl")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(10))
                    .withDescription("the cache time to live.");
    private static final ConfigOption<Integer> LOOKUP_MAX_RETRIES =
            ConfigOptions.key("lookup.max-retries")
                    .intType()
                    .defaultValue(3)
                    .withDescription("the max retry times if lookup database failed.");

    // write config options
    private static final ConfigOption<Integer> SINK_BUFFER_FLUSH_MAX_ROWS =
            ConfigOptions.key("sink.buffer-flush.max-rows")
                    .intType()
                    .defaultValue(100)
                    .withDescription(
                            "the flush max size (includes all append, upsert and delete records), over this number"
                                    + " of records, will flush data. The default value is 100.");
    private static final ConfigOption<Duration> SINK_BUFFER_FLUSH_INTERVAL =
            ConfigOptions.key("sink.buffer-flush.interval")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(1))
                    .withDescription(
                            "the flush interval mills, over this time, asynchronous threads will flush data. The "
                                    + "default value is 1s.");
    private static final ConfigOption<Integer> SINK_MAX_RETRIES =
            ConfigOptions.key("sink.max-retries")
                    .intType()
                    .defaultValue(3)
                    .withDescription("the max retry times if writing records to database failed.");

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(PGWalOptions.JDBC_URL_CONFIG_OPTION);
        options.add(PGWalOptions.USERNAME_CONFIG_OPTION);
        options.add(PGWalOptions.PASSWORD_CONFIG_OPTION);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(PGWalOptions.ALLOW_CREATE_SLOT_CONFIG_OPTION);
        options.add(PGWalOptions.CATALOG_CONFIG_OPTION);
        options.add(PGWalOptions.DATABASE_CONFIG_OPTION);
        options.add(PGWalOptions.LSN_CONFIG_OPTION);
        options.add(PGWalOptions.PASSWORD_CONFIG_OPTION);
        options.add(PGWalOptions.PAVING_CONFIG_OPTION);
        options.add(PGWalOptions.SLOT_NAME_CONFIG_OPTION);
        options.add(PGWalOptions.STATUS_INTERVAL_CONFIG_OPTION);
        options.add(PGWalOptions.TABLES_CONFIG_OPTION);
        options.add(PGWalOptions.TEMPORARY_CONFIG_OPTION);
        options.add(JsonOptions.TIMESTAMP_FORMAT);
        return options;
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        final ReadableConfig config = helper.getOptions();

        helper.validate();
        validateConfigOptions(config);
        TableSchema physicalSchema =
                TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());
        return new JdbcDynamicTableSource(
                getJdbcOptions(helper.getOptions()),
                getJdbcReadOptions(helper.getOptions()),
                getJdbcLookupOptions(helper.getOptions()),
                physicalSchema) {
            @Override
            public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
                final RowType rowType = (RowType) physicalSchema.toRowDataType().getLogicalType();
                TypeInformation<RowData> typeInformation = InternalTypeInfo.of(rowType);

                //                JdbcInputFormatBuilder builder = new JdbcInputFormatBuilder(new
                // JdbcInputFormat());
                //                String[] fieldNames = physicalSchema.getFieldNames();
                //                List<FieldConf> columnList = new ArrayList<>(fieldNames.length);
                //                int index = 0;
                //                for (String name : fieldNames) {
                //                    FieldConf field = new FieldConf();
                //                    field.setName(name);
                //                    field.setIndex(index++);
                //                    columnList.add(field);
                //                }
                //                jdbcConf.setColumn(columnList);
                //
                //                String restoreColumn = jdbcConf.getRestoreColumn();
                //                if(StringUtils.isNotBlank(restoreColumn)){
                //                    FieldConf fieldConf =
                // FieldConf.getSameNameMetaColumn(jdbcConf.getColumn(), restoreColumn);
                //                    if (fieldConf != null) {
                //                        jdbcConf.setRestoreColumn(restoreColumn);
                //                        jdbcConf.setRestoreColumnIndex(fieldConf.getIndex());
                //
                // jdbcConf.setRestoreColumnType(jdbcConf.getIncreColumnType());
                //                    } else {
                //                        throw new IllegalArgumentException("unknown restore column
                // name: " + restoreColumn);
                //                    }
                //                }
                //
                //                builder.setJdbcDialect(jdbcDialect);
                //                builder.setJdbcConf(jdbcConf);
                //                builder.setRowConverter(jdbcDialect.getRowConverter(rowType));

                PGWalConf conf = new PGWalConf();

                PGCDCServiceProcessor serviceProcessor = new PGCDCServiceProcessor();
                Map<String, Object> params = new HashMap<>();
                params.put("conf", conf);
                try {
                    serviceProcessor.init(params);
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
                return ParallelSourceFunctionProvider.of(
                        new DtInputFormatSourceFunction<>(serviceProcessor, typeInformation),
                        false,
                        conf.getParallelism());
            }
        };
    }

    private void validateConfigOptions(ReadableConfig config) {
        String jdbcUrl = config.get(URL);
        final Optional<JdbcDialect> dialect = JdbcDialects.get(jdbcUrl);
        checkState(dialect.isPresent(), "Cannot handle such jdbc url: " + jdbcUrl);

        checkAllOrNone(config, new ConfigOption[] {USERNAME, PASSWORD});

        checkAllOrNone(
                config,
                new ConfigOption[] {
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

        checkAllOrNone(config, new ConfigOption[] {LOOKUP_CACHE_MAX_ROWS, LOOKUP_CACHE_TTL});

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

    private JdbcOptions getJdbcOptions(ReadableConfig readableConfig) {
        final String url = readableConfig.get(URL);
        final JdbcOptions.Builder builder =
                JdbcOptions.builder()
                        .setDBUrl(url)
                        .setTableName(readableConfig.get(TABLE_NAME))
                        .setDialect(JdbcDialects.get(url).get());

        readableConfig.getOptional(DRIVER).ifPresent(builder::setDriverName);
        readableConfig.getOptional(USERNAME).ifPresent(builder::setUsername);
        readableConfig.getOptional(PASSWORD).ifPresent(builder::setPassword);
        return builder.build();
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

    private JdbcReadOptions getJdbcReadOptions(ReadableConfig readableConfig) {
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

    private JdbcLookupOptions getJdbcLookupOptions(ReadableConfig readableConfig) {
        return new JdbcLookupOptions(
                readableConfig.get(LOOKUP_CACHE_MAX_ROWS),
                readableConfig.get(LOOKUP_CACHE_TTL).toMillis(),
                readableConfig.get(LOOKUP_MAX_RETRIES));
    }

    private PGWalConf getConf(ReadableConfig config) {
        PGWalConf conf = new PGWalConf();
        conf.setCredentials(
                config.get(PGWalOptions.USERNAME_CONFIG_OPTION),
                config.get(PGWalOptions.PASSWORD_CONFIG_OPTION));
        conf.setJdbcUrl(config.get(PGWalOptions.JDBC_URL_CONFIG_OPTION));
        conf.setNamespace(
                config.get(PGWalOptions.CATALOG_CONFIG_OPTION),
                config.get(PGWalOptions.DATABASE_CONFIG_OPTION));
        conf.setPavingData(config.get(PGWalOptions.PAVING_CONFIG_OPTION));
        conf.setTableList(config.get(PGWalOptions.TABLES_CONFIG_OPTION));
        conf.setStatusInterval(config.get(PGWalOptions.STATUS_INTERVAL_CONFIG_OPTION));
        conf.setLsn(config.get(PGWalOptions.LSN_CONFIG_OPTION));
        conf.setSlotAttribute(
                config.get(PGWalOptions.SLOT_NAME_CONFIG_OPTION),
                config.get(PGWalOptions.ALLOW_CREATE_SLOT_CONFIG_OPTION),
                config.get(PGWalOptions.TEMPORARY_CONFIG_OPTION));
        return conf;
    }
}
