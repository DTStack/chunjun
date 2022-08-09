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
package com.dtstack.chunjun.connector.inceptor.table;

import com.dtstack.chunjun.connector.inceptor.conf.InceptorConf;
import com.dtstack.chunjun.connector.inceptor.dialect.InceptorDialect;
import com.dtstack.chunjun.connector.inceptor.options.InceptorOptions;
import com.dtstack.chunjun.connector.inceptor.sink.InceptorDynamicTableSink;
import com.dtstack.chunjun.connector.inceptor.source.InceptorDynamicTableSource;
import com.dtstack.chunjun.connector.inceptor.util.InceptorDbUtil;
import com.dtstack.chunjun.connector.jdbc.conf.SinkConnectionConf;
import com.dtstack.chunjun.connector.jdbc.conf.SourceConnectionConf;
import com.dtstack.chunjun.connector.jdbc.dialect.JdbcDialect;
import com.dtstack.chunjun.connector.jdbc.sink.JdbcOutputFormatBuilder;
import com.dtstack.chunjun.connector.jdbc.source.JdbcInputFormatBuilder;
import com.dtstack.chunjun.connector.jdbc.table.JdbcDynamicTableFactory;
import com.dtstack.chunjun.connector.jdbc.util.JdbcUtil;
import com.dtstack.chunjun.enums.Semantic;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.constraints.UniqueConstraint;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.utils.TableSchemaUtils;

import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.dtstack.chunjun.connector.jdbc.options.JdbcCommonOptions.PASSWORD;
import static com.dtstack.chunjun.connector.jdbc.options.JdbcCommonOptions.SCHEMA;
import static com.dtstack.chunjun.connector.jdbc.options.JdbcCommonOptions.TABLE_NAME;
import static com.dtstack.chunjun.connector.jdbc.options.JdbcCommonOptions.URL;
import static com.dtstack.chunjun.connector.jdbc.options.JdbcCommonOptions.USERNAME;
import static com.dtstack.chunjun.connector.jdbc.options.JdbcLookupOptions.DRUID_PREFIX;
import static com.dtstack.chunjun.connector.jdbc.options.JdbcLookupOptions.LOOKUP_CACHE_MAX_ROWS;
import static com.dtstack.chunjun.connector.jdbc.options.JdbcLookupOptions.LOOKUP_CACHE_TTL;
import static com.dtstack.chunjun.connector.jdbc.options.JdbcLookupOptions.LOOKUP_MAX_RETRIES;
import static com.dtstack.chunjun.connector.jdbc.options.JdbcLookupOptions.VERTX_PREFIX;
import static com.dtstack.chunjun.connector.jdbc.options.JdbcLookupOptions.getLibConfMap;
import static com.dtstack.chunjun.connector.jdbc.options.JdbcSinkOptions.SINK_ALL_REPLACE;
import static com.dtstack.chunjun.connector.jdbc.options.JdbcSinkOptions.SINK_SEMANTIC;
import static com.dtstack.chunjun.source.options.SourceOptions.SCAN_DEFAULT_FETCH_SIZE;
import static com.dtstack.chunjun.source.options.SourceOptions.SCAN_FETCH_SIZE;
import static com.dtstack.chunjun.source.options.SourceOptions.SCAN_INCREMENT_COLUMN;
import static com.dtstack.chunjun.source.options.SourceOptions.SCAN_INCREMENT_COLUMN_TYPE;
import static com.dtstack.chunjun.source.options.SourceOptions.SCAN_PARALLELISM;
import static com.dtstack.chunjun.source.options.SourceOptions.SCAN_PARTITION_COLUMN;
import static com.dtstack.chunjun.source.options.SourceOptions.SCAN_PARTITION_STRATEGY;
import static com.dtstack.chunjun.source.options.SourceOptions.SCAN_POLLING_INTERVAL;
import static com.dtstack.chunjun.source.options.SourceOptions.SCAN_QUERY_TIMEOUT;
import static com.dtstack.chunjun.source.options.SourceOptions.SCAN_RESTORE_COLUMNNAME;
import static com.dtstack.chunjun.source.options.SourceOptions.SCAN_RESTORE_COLUMNTYPE;
import static com.dtstack.chunjun.source.options.SourceOptions.SCAN_START_LOCATION;
import static com.dtstack.chunjun.table.options.SinkOptions.SINK_BUFFER_FLUSH_INTERVAL;
import static com.dtstack.chunjun.table.options.SinkOptions.SINK_BUFFER_FLUSH_MAX_ROWS;
import static com.dtstack.chunjun.table.options.SinkOptions.SINK_MAX_RETRIES;
import static com.dtstack.chunjun.table.options.SinkOptions.SINK_PARALLELISM;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Date: 2021/06/22 Company: www.dtstack.com
 *
 * @author dujie
 */
public class InceptorDynamicTableFactory extends JdbcDynamicTableFactory {
    public static final String IDENTIFIER = "inceptor-x";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    protected JdbcDialect getDialect() {
        return null;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> requiredOptions = super.requiredOptions();
        // 必须是分区表 指定分区
        requiredOptions.add(InceptorOptions.PARTITION);
        requiredOptions.add(InceptorOptions.PARTITION_TYPE);
        return requiredOptions;
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        // 1.所有的requiredOptions和optionalOptions参数
        final ReadableConfig config = helper.getOptions();

        // 2.参数校验
        helper.validateExcept(VERTX_PREFIX, DRUID_PREFIX, "security.kerberos.");
        validateConfigOptions(config);
        // 3.封装参数
        TableSchema physicalSchema =
                TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());

        final Map<String, Object> druidConf =
                getLibConfMap(context.getCatalogTable().getOptions(), DRUID_PREFIX);

        InceptorConf sourceConnectionConf = getSourceConnectionConf(helper.getOptions());

        addKerberosConfig(sourceConnectionConf, context.getCatalogTable().getOptions());

        InceptorDialect inceptorDialect =
                InceptorDbUtil.getDialectWithDriverType(sourceConnectionConf);

        return new InceptorDynamicTableSource(
                sourceConnectionConf,
                getJdbcLookupConf(
                        helper.getOptions(),
                        context.getObjectIdentifier().getObjectName(),
                        druidConf),
                physicalSchema,
                inceptorDialect,
                inceptorDialect.getInputFormatBuilder());
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        // 1.所有的requiredOptions和optionalOptions参数
        final ReadableConfig config = helper.getOptions();

        // 2.参数校验
        helper.validateExcept("properties.", "security.kerberos");

        // 3.封装参数
        TableSchema physicalSchema =
                TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());

        InceptorConf inceptorConf = getSinkConnectionConf(config, physicalSchema);

        addKerberosConfig(inceptorConf, context.getCatalogTable().getOptions());

        InceptorDialect inceptorDialect = InceptorDbUtil.getDialectWithDriverType(inceptorConf);

        return new InceptorDynamicTableSink(
                inceptorDialect,
                physicalSchema,
                inceptorDialect.getOutputFormatBuilder(),
                inceptorConf);
    }

    @Override
    protected InceptorConf getSinkConnectionConf(
            ReadableConfig readableConfig, TableSchema schema) {
        InceptorConf inceptorConf = new InceptorConf();

        SinkConnectionConf sinkConnectionConf = new SinkConnectionConf();
        inceptorConf.setConnection(Collections.singletonList(sinkConnectionConf));

        sinkConnectionConf.setJdbcUrl(readableConfig.get(URL));
        sinkConnectionConf.setTable(Collections.singletonList(readableConfig.get(TABLE_NAME)));
        sinkConnectionConf.setSchema(readableConfig.get(SCHEMA));
        sinkConnectionConf.setAllReplace(readableConfig.get(SINK_ALL_REPLACE));
        sinkConnectionConf.setUsername(readableConfig.get(USERNAME));
        sinkConnectionConf.setPassword(readableConfig.get(PASSWORD));

        inceptorConf.setPartition(readableConfig.get(InceptorOptions.PARTITION));
        inceptorConf.setPartitionType(readableConfig.get(InceptorOptions.PARTITION_TYPE));

        inceptorConf.setUsername(readableConfig.get(USERNAME));
        inceptorConf.setPassword(readableConfig.get(PASSWORD));

        inceptorConf.setAllReplace(sinkConnectionConf.getAllReplace());
        inceptorConf.setBatchSize(readableConfig.get(SINK_BUFFER_FLUSH_MAX_ROWS));
        inceptorConf.setFlushIntervalMills(readableConfig.get(SINK_BUFFER_FLUSH_INTERVAL));
        inceptorConf.setParallelism(readableConfig.get(SINK_PARALLELISM));
        inceptorConf.setSemantic(readableConfig.get(SINK_SEMANTIC));

        List<String> keyFields =
                schema.getPrimaryKey().map(UniqueConstraint::getColumns).orElse(null);
        inceptorConf.setUniqueKey(keyFields);
        resetTableInfo(inceptorConf);
        JdbcUtil.putExtParam(inceptorConf);
        return inceptorConf;
    }

    @Override
    protected InceptorConf getSourceConnectionConf(ReadableConfig readableConfig) {
        InceptorConf jdbcConf = new InceptorConf();
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

        resetTableInfo(jdbcConf);
        return jdbcConf;
    }

    @Override
    protected JdbcOutputFormatBuilder getOutputFormatBuilder() {
        return null;
    }

    @Override
    protected JdbcInputFormatBuilder getInputFormatBuilder() {
        return null;
    }

    protected void validateConfigOptions(ReadableConfig config) {
        String jdbcUrl = config.get(URL);
        checkState(true, "Cannot handle such jdbc url: " + jdbcUrl);

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

    private void addKerberosConfig(InceptorConf inceptorConf, Map<String, String> options) {
        Map<String, Object> kerberosConfig = InceptorOptions.getKerberosConfig(options);
        kerberosConfig.put("useLocalFile", true);
        inceptorConf.setHadoopConfig(kerberosConfig);
    }
}
