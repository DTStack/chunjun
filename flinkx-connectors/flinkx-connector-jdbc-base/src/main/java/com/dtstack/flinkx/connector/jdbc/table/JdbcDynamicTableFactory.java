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

import com.dtstack.flinkx.connector.jdbc.conf.JdbcConf;
import com.dtstack.flinkx.connector.jdbc.conf.JdbcLookupConf;
import com.dtstack.flinkx.connector.jdbc.conf.SinkConnectionConf;
import com.dtstack.flinkx.connector.jdbc.conf.SourceConnectionConf;
import com.dtstack.flinkx.connector.jdbc.dialect.JdbcDialect;
import com.dtstack.flinkx.connector.jdbc.sink.JdbcDynamicTableSink;
import com.dtstack.flinkx.connector.jdbc.sink.JdbcOutputFormat;
import com.dtstack.flinkx.connector.jdbc.sink.JdbcOutputFormatBuilder;
import com.dtstack.flinkx.connector.jdbc.source.JdbcDynamicTableSource;
import com.dtstack.flinkx.connector.jdbc.source.JdbcInputFormat;
import com.dtstack.flinkx.connector.jdbc.source.JdbcInputFormatBuilder;
import com.dtstack.flinkx.connector.jdbc.util.JdbcUtil;
import com.dtstack.flinkx.enums.Semantic;
import com.dtstack.flinkx.lookup.conf.LookupConf;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.constraints.UniqueConstraint;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.util.Preconditions;

import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.dtstack.flinkx.connector.jdbc.options.JdbcCommonOptions.PASSWORD;
import static com.dtstack.flinkx.connector.jdbc.options.JdbcCommonOptions.SCHEMA;
import static com.dtstack.flinkx.connector.jdbc.options.JdbcCommonOptions.TABLE_NAME;
import static com.dtstack.flinkx.connector.jdbc.options.JdbcCommonOptions.URL;
import static com.dtstack.flinkx.connector.jdbc.options.JdbcCommonOptions.USERNAME;
import static com.dtstack.flinkx.connector.jdbc.options.JdbcLookupOptions.DRUID_PREFIX;
import static com.dtstack.flinkx.connector.jdbc.options.JdbcLookupOptions.VERTX_PREFIX;
import static com.dtstack.flinkx.connector.jdbc.options.JdbcLookupOptions.VERTX_WORKER_POOL_SIZE;
import static com.dtstack.flinkx.connector.jdbc.options.JdbcLookupOptions.getLibConfMap;
import static com.dtstack.flinkx.connector.jdbc.options.JdbcSinkOptions.SINK_ALL_REPLACE;
import static com.dtstack.flinkx.connector.jdbc.options.JdbcSinkOptions.SINK_PARALLELISM;
import static com.dtstack.flinkx.connector.jdbc.options.JdbcSinkOptions.SINK_SEMANTIC;
import static com.dtstack.flinkx.lookup.options.LookupOptions.LOOKUP_ASYNC_TIMEOUT;
import static com.dtstack.flinkx.lookup.options.LookupOptions.LOOKUP_CACHE_MAX_ROWS;
import static com.dtstack.flinkx.lookup.options.LookupOptions.LOOKUP_CACHE_PERIOD;
import static com.dtstack.flinkx.lookup.options.LookupOptions.LOOKUP_CACHE_TTL;
import static com.dtstack.flinkx.lookup.options.LookupOptions.LOOKUP_CACHE_TYPE;
import static com.dtstack.flinkx.lookup.options.LookupOptions.LOOKUP_ERROR_LIMIT;
import static com.dtstack.flinkx.lookup.options.LookupOptions.LOOKUP_FETCH_SIZE;
import static com.dtstack.flinkx.lookup.options.LookupOptions.LOOKUP_MAX_RETRIES;
import static com.dtstack.flinkx.lookup.options.LookupOptions.LOOKUP_PARALLELISM;
import static com.dtstack.flinkx.source.options.SourceOptions.SCAN_CONNECTION_QUERY_TIMEOUT;
import static com.dtstack.flinkx.source.options.SourceOptions.SCAN_DEFAULT_FETCH_SIZE;
import static com.dtstack.flinkx.source.options.SourceOptions.SCAN_FETCH_SIZE;
import static com.dtstack.flinkx.source.options.SourceOptions.SCAN_INCREMENT_COLUMN;
import static com.dtstack.flinkx.source.options.SourceOptions.SCAN_INCREMENT_COLUMN_TYPE;
import static com.dtstack.flinkx.source.options.SourceOptions.SCAN_PARALLELISM;
import static com.dtstack.flinkx.source.options.SourceOptions.SCAN_PARTITION_COLUMN;
import static com.dtstack.flinkx.source.options.SourceOptions.SCAN_PARTITION_STRATEGY;
import static com.dtstack.flinkx.source.options.SourceOptions.SCAN_POLLING_INTERVAL;
import static com.dtstack.flinkx.source.options.SourceOptions.SCAN_QUERY_TIMEOUT;
import static com.dtstack.flinkx.source.options.SourceOptions.SCAN_RESTORE_COLUMNNAME;
import static com.dtstack.flinkx.source.options.SourceOptions.SCAN_RESTORE_COLUMNTYPE;
import static com.dtstack.flinkx.source.options.SourceOptions.SCAN_START_LOCATION;
import static com.dtstack.flinkx.table.options.SinkOptions.SINK_BUFFER_FLUSH_INTERVAL;
import static com.dtstack.flinkx.table.options.SinkOptions.SINK_BUFFER_FLUSH_MAX_ROWS;
import static com.dtstack.flinkx.table.options.SinkOptions.SINK_CONNECTION_QUERY_TIMEOUT;
import static com.dtstack.flinkx.table.options.SinkOptions.SINK_MAX_RETRIES;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * @author chuixue
 * @create 2021-04-10 12:54
 * @description
 */
public abstract class JdbcDynamicTableFactory
        implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        // 1.所有的requiredOptions和optionalOptions参数
        final ReadableConfig config = helper.getOptions();

        // 2.参数校验
        helper.validateExcept(VERTX_PREFIX, DRUID_PREFIX);
        validateConfigOptions(config);
        // 3.封装参数
        TableSchema physicalSchema =
                TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());
        JdbcDialect jdbcDialect = getDialect();

        final Map<String, Object> druidConf =
                getLibConfMap(context.getCatalogTable().getOptions(), DRUID_PREFIX);

        return new JdbcDynamicTableSource(
                getSourceConnectionConf(helper.getOptions()),
                getJdbcLookupConf(
                        helper.getOptions(),
                        context.getObjectIdentifier().getObjectName(),
                        druidConf),
                physicalSchema,
                jdbcDialect,
                getInputFormatBuilder());
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        // 1.所有的requiredOptions和optionalOptions参数
        final ReadableConfig config = helper.getOptions();

        // 2.参数校验
        helper.validate();
        validateConfigOptions(config);
        JdbcDialect jdbcDialect = getDialect();

        // 3.封装参数
        TableSchema physicalSchema =
                TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());

        return new JdbcDynamicTableSink(
                getSinkConnectionConf(helper.getOptions(), physicalSchema),
                jdbcDialect,
                physicalSchema,
                getOutputFormatBuilder());
    }

    protected JdbcConf getSinkConnectionConf(ReadableConfig readableConfig, TableSchema schema) {
        JdbcConf jdbcConf = new JdbcConf();
        SinkConnectionConf conf = new SinkConnectionConf();
        jdbcConf.setConnection(Collections.singletonList(conf));

        conf.setJdbcUrl(readableConfig.get(URL));
        conf.setTable(Arrays.asList(readableConfig.get(TABLE_NAME)));
        conf.setSchema(readableConfig.get(SCHEMA));
        conf.setAllReplace(readableConfig.get(SINK_ALL_REPLACE));

        jdbcConf.setUsername(readableConfig.get(USERNAME));
        jdbcConf.setPassword(readableConfig.get(PASSWORD));

        jdbcConf.setAllReplace(conf.getAllReplace());
        jdbcConf.setBatchSize(readableConfig.get(SINK_BUFFER_FLUSH_MAX_ROWS));
        jdbcConf.setFlushIntervalMills(readableConfig.get(SINK_BUFFER_FLUSH_INTERVAL));
        jdbcConf.setParallelism(readableConfig.get(SINK_PARALLELISM));
        jdbcConf.setSemantic(readableConfig.get(SINK_SEMANTIC));

        jdbcConf.setConnectTimeOut(readableConfig.get(SINK_CONNECTION_QUERY_TIMEOUT));

        List<String> keyFields =
                schema.getPrimaryKey().map(UniqueConstraint::getColumns).orElse(null);
        jdbcConf.setUniqueKey(keyFields);
        rebuildJdbcConf(jdbcConf);
        return jdbcConf;
    }

    protected LookupConf getJdbcLookupConf(
            ReadableConfig readableConfig, String tableName, Map<String, Object> druidConf) {
        return JdbcLookupConf.build()
                .setDruidConf(druidConf)
                .setAsyncPoolSize(readableConfig.get(VERTX_WORKER_POOL_SIZE))
                .setTableName(tableName)
                .setPeriod(readableConfig.get(LOOKUP_CACHE_PERIOD))
                .setCacheSize(readableConfig.get(LOOKUP_CACHE_MAX_ROWS))
                .setCacheTtl(readableConfig.get(LOOKUP_CACHE_TTL))
                .setCache(readableConfig.get(LOOKUP_CACHE_TYPE))
                .setMaxRetryTimes(readableConfig.get(LOOKUP_MAX_RETRIES))
                .setErrorLimit(readableConfig.get(LOOKUP_ERROR_LIMIT))
                .setFetchSize(readableConfig.get(LOOKUP_FETCH_SIZE))
                .setAsyncTimeout(readableConfig.get(LOOKUP_ASYNC_TIMEOUT))
                .setParallelism(readableConfig.get(LOOKUP_PARALLELISM));
    }

    protected JdbcConf getSourceConnectionConf(ReadableConfig readableConfig) {
        JdbcConf jdbcConf = new JdbcConf();
        SourceConnectionConf conf = new SourceConnectionConf();
        jdbcConf.setConnection(Collections.singletonList(conf));

        conf.setJdbcUrl(Arrays.asList(readableConfig.get(URL)));
        conf.setTable(Arrays.asList(readableConfig.get(TABLE_NAME)));
        conf.setSchema(readableConfig.get(SCHEMA));

        jdbcConf.setJdbcUrl(readableConfig.get(URL));
        jdbcConf.setUsername(readableConfig.get(USERNAME));
        jdbcConf.setPassword(readableConfig.get(PASSWORD));

        jdbcConf.setParallelism(readableConfig.get(SCAN_PARALLELISM));
        jdbcConf.setFetchSize(
                readableConfig.get(SCAN_FETCH_SIZE) == 0
                        ? getDefaultFetchSize()
                        : readableConfig.get(SCAN_FETCH_SIZE));
        jdbcConf.setQueryTimeOut(readableConfig.get(SCAN_QUERY_TIMEOUT));
        jdbcConf.setConnectTimeOut(readableConfig.get(SCAN_CONNECTION_QUERY_TIMEOUT));

        jdbcConf.setSplitPk(readableConfig.get(SCAN_PARTITION_COLUMN));
        jdbcConf.setSplitStrategy(readableConfig.get(SCAN_PARTITION_STRATEGY));

        String increColumn = readableConfig.get(SCAN_INCREMENT_COLUMN);
        if (StringUtils.isNotBlank(increColumn)) {
            jdbcConf.setIncrement(true);
            jdbcConf.setIncreColumn(increColumn);
            jdbcConf.setIncreColumnType(readableConfig.get(SCAN_INCREMENT_COLUMN_TYPE));
        }

        jdbcConf.setStartLocation(readableConfig.get(SCAN_START_LOCATION));

        jdbcConf.setRestoreColumn(readableConfig.get(SCAN_RESTORE_COLUMNNAME));
        jdbcConf.setRestoreColumnType(readableConfig.get(SCAN_RESTORE_COLUMNTYPE));

        Optional<Integer> pollingInterval = readableConfig.getOptional(SCAN_POLLING_INTERVAL);
        if (pollingInterval.isPresent() && pollingInterval.get() > 0) {
            jdbcConf.setPolling(true);
            jdbcConf.setPollingInterval(pollingInterval.get());
            jdbcConf.setFetchSize(
                    readableConfig.get(SCAN_FETCH_SIZE) == 0
                            ? SCAN_DEFAULT_FETCH_SIZE.defaultValue()
                            : readableConfig.get(SCAN_FETCH_SIZE));
        }

        rebuildJdbcConf(jdbcConf);
        return jdbcConf;
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
        optionalOptions.add(SCHEMA);

        optionalOptions.add(SCAN_PARTITION_COLUMN);
        optionalOptions.add(SCAN_PARTITION_STRATEGY);
        optionalOptions.add(SCAN_INCREMENT_COLUMN);
        optionalOptions.add(SCAN_INCREMENT_COLUMN_TYPE);
        optionalOptions.add(SCAN_POLLING_INTERVAL);
        optionalOptions.add(SCAN_START_LOCATION);
        optionalOptions.add(SCAN_PARALLELISM);
        optionalOptions.add(SCAN_QUERY_TIMEOUT);
        optionalOptions.add(SCAN_CONNECTION_QUERY_TIMEOUT);
        optionalOptions.add(SCAN_FETCH_SIZE);
        optionalOptions.add(SCAN_RESTORE_COLUMNNAME);
        optionalOptions.add(SCAN_RESTORE_COLUMNTYPE);

        optionalOptions.add(LOOKUP_CACHE_PERIOD);
        optionalOptions.add(LOOKUP_CACHE_MAX_ROWS);
        optionalOptions.add(LOOKUP_CACHE_TTL);
        optionalOptions.add(LOOKUP_CACHE_TYPE);
        optionalOptions.add(LOOKUP_MAX_RETRIES);
        optionalOptions.add(LOOKUP_ERROR_LIMIT);
        optionalOptions.add(LOOKUP_FETCH_SIZE);
        optionalOptions.add(LOOKUP_ASYNC_TIMEOUT);
        optionalOptions.add(LOOKUP_PARALLELISM);

        optionalOptions.add(SINK_BUFFER_FLUSH_MAX_ROWS);
        optionalOptions.add(SINK_BUFFER_FLUSH_INTERVAL);
        optionalOptions.add(SINK_MAX_RETRIES);
        optionalOptions.add(SINK_ALL_REPLACE);
        optionalOptions.add(SINK_PARALLELISM);
        optionalOptions.add(SINK_SEMANTIC);
        optionalOptions.add(SINK_CONNECTION_QUERY_TIMEOUT);
        return optionalOptions;
    }

    protected void validateConfigOptions(ReadableConfig config) {
        String jdbcUrl = config.get(URL);
        final Optional<JdbcDialect> dialect = Optional.of(getDialect());
        checkState(true, "Cannot handle such jdbc url: " + jdbcUrl);

        checkAllOrNone(config, new ConfigOption[] {USERNAME, PASSWORD});

        if (config.getOptional(SCAN_POLLING_INTERVAL).isPresent()
                && config.getOptional(SCAN_POLLING_INTERVAL).get() > 0) {
            checkState(
                    StringUtils.isNotBlank(config.get(SCAN_INCREMENT_COLUMN)),
                    "scan.increment.column can not null or empty in polling-interval mode.");
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
        try {
            Semantic.getByName(config.get(SINK_SEMANTIC));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    String.format(
                            "The value of '%s' option should only be %s or %s, but is %s.",
                            SINK_SEMANTIC.key(),
                            Semantic.EXACTLY_ONCE.getAlisName(),
                            Semantic.AT_LEAST_ONCE.getAlisName(),
                            config.get(SINK_SEMANTIC)));
        }
    }

    protected void checkAllOrNone(ReadableConfig config, ConfigOption<?>[] configOptions) {
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
     * 从数据库中每次读取的条数，离线读取的时候每个插件需要测试，防止大数据量下生成大量临时文件
     *
     * @return
     */
    protected int getDefaultFetchSize() {
        return SCAN_DEFAULT_FETCH_SIZE.defaultValue();
    }

    /**
     * 获取JDBC插件的具体inputFormatBuilder
     *
     * @return JdbcInputFormatBuilder
     */
    protected JdbcInputFormatBuilder getInputFormatBuilder() {
        return new JdbcInputFormatBuilder(new JdbcInputFormat());
    }

    /**
     * 获取JDBC插件的具体outputFormatBuilder
     *
     * @return JdbcOutputFormatBuilder
     */
    protected JdbcOutputFormatBuilder getOutputFormatBuilder() {
        return new JdbcOutputFormatBuilder(new JdbcOutputFormat());
    }

    /** rebuild jdbcConf,add custom configuration */
    protected void rebuildJdbcConf(JdbcConf jdbcConf) {
        // table字段有可能是schema.table格式 需要转换为对应的schema 和 table 字段
        if (StringUtils.isBlank(jdbcConf.getSchema())) {
            JdbcUtil.resetSchemaAndTable(jdbcConf, "\\\"", "\\\"");
        }
    }
}
