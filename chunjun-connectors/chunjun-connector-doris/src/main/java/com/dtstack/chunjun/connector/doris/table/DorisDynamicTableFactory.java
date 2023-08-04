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

package com.dtstack.chunjun.connector.doris.table;

import com.dtstack.chunjun.connector.doris.DorisUtil;
import com.dtstack.chunjun.connector.doris.options.DorisConfig;
import com.dtstack.chunjun.connector.doris.options.DorisOptions;
import com.dtstack.chunjun.connector.doris.options.LoadConfig;
import com.dtstack.chunjun.connector.doris.sink.DorisDynamicTableSink;
import com.dtstack.chunjun.connector.doris.source.DorisInputFormat;
import com.dtstack.chunjun.connector.doris.source.DorisInputFormatBuilder;
import com.dtstack.chunjun.connector.jdbc.config.JdbcConfig;
import com.dtstack.chunjun.connector.jdbc.dialect.JdbcDialect;
import com.dtstack.chunjun.connector.jdbc.source.JdbcDynamicTableSource;
import com.dtstack.chunjun.connector.jdbc.source.JdbcInputFormatBuilder;
import com.dtstack.chunjun.connector.jdbc.table.JdbcDynamicTableFactory;
import com.dtstack.chunjun.connector.mysql.dialect.MysqlDialect;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;

import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.dtstack.chunjun.connector.jdbc.options.JdbcLookupOptions.DRUID_PREFIX;
import static com.dtstack.chunjun.connector.jdbc.options.JdbcLookupOptions.VERTX_PREFIX;
import static com.dtstack.chunjun.connector.jdbc.options.JdbcLookupOptions.getLibConfMap;
import static com.dtstack.chunjun.connector.jdbc.options.JdbcSinkOptions.SINK_POST_SQL;
import static com.dtstack.chunjun.connector.jdbc.options.JdbcSinkOptions.SINK_PRE_SQL;

/** declare doris table factory info. */
public class DorisDynamicTableFactory extends JdbcDynamicTableFactory
        implements DynamicTableSinkFactory {

    private static final String IDENTIFIER = "doris-x";

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        // 1.所有的requiredOptions和optionalOptions参数
        final ReadableConfig config = helper.getOptions();

        // 2.参数校验
        helper.validate();

        String url = config.get(DorisOptions.URL);
        List<String> feNodes = config.get(DorisOptions.FENODES);

        if (StringUtils.isEmpty(url) && (null == feNodes || feNodes.isEmpty())) {
            throw new IllegalArgumentException(
                    "Choose one of 'url' and 'feNodes', them can not be empty at same time.");
        }

        // 3.封装参数
        ResolvedSchema resolvedSchema = context.getCatalogTable().getResolvedSchema();

        DorisConfig ftpConfig = getConfByOptions(config);
        return new DorisDynamicTableSink(resolvedSchema, ftpConfig);
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {

        DorisInputFormat dorisInputFormat = new DorisInputFormat();

        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        // 1.所有的requiredOptions和optionalOptions参数
        final ReadableConfig config = helper.getOptions();

        DorisConfig dorisConfig = new DorisConfig();
        dorisConfig.setUrl(config.get(DorisOptions.URL));
        dorisConfig.setFeNodes(config.get(DorisOptions.FENODES));

        dorisInputFormat.setDorisConfig(dorisConfig);

        // 2.参数校验
        helper.validateExcept(VERTX_PREFIX, DRUID_PREFIX);
        ResolvedSchema resolvedSchema = context.getCatalogTable().getResolvedSchema();
        validateConfigOptions(config, resolvedSchema);
        // 3.封装参数
        JdbcDialect jdbcDialect = getDialect();

        final Map<String, Object> druidConf =
                getLibConfMap(context.getCatalogTable().getOptions(), DRUID_PREFIX);

        return new JdbcDynamicTableSource(
                getSourceConnectionConfig(helper.getOptions()),
                getJdbcLookupConfig(
                        helper.getOptions(),
                        context.getObjectIdentifier().getObjectName(),
                        druidConf),
                resolvedSchema,
                jdbcDialect,
                new DorisInputFormatBuilder(dorisInputFormat));
    }

    @Override
    protected JdbcConfig getSourceConnectionConfig(ReadableConfig readableConfig) {
        JdbcConfig jdbcConfig = super.getSourceConnectionConfig(readableConfig);
        String url = readableConfig.get(DorisOptions.URL);
        List<String> feNodes = readableConfig.get(DorisOptions.FENODES);

        String jdbcUrl = DorisUtil.getJdbcUrlFromFe(feNodes, url);

        jdbcConfig.setJdbcUrl(jdbcUrl);

        return jdbcConfig;
    }

    @Override
    protected void validateConfigOptions(ReadableConfig config, ResolvedSchema tableSchema) {
        String url = config.get(DorisOptions.URL);
        List<String> feNodes = config.get(DorisOptions.FENODES);

        if (StringUtils.isEmpty(url) && (null == feNodes || feNodes.isEmpty())) {
            throw new IllegalArgumentException(
                    "Choose one of 'url' and 'feNodes', them can not be empty at same time.");
        }
    }

    private static DorisConfig getConfByOptions(ReadableConfig config) {
        DorisConfig dorisConfig = new DorisConfig();
        if (StringUtils.isNotEmpty(config.get(SINK_PRE_SQL))) {
            dorisConfig.setPreSql(Arrays.asList(config.get(SINK_PRE_SQL).split(";")));
        }
        if (StringUtils.isNotEmpty(config.get(SINK_POST_SQL))) {
            dorisConfig.setPostSql(Arrays.asList(config.get(SINK_POST_SQL).split(";")));
        }

        dorisConfig.setFeNodes(config.get(DorisOptions.FENODES));

        String schema = config.get(DorisOptions.SCHEMA);
        String tableName = config.get(DorisOptions.TABLE_NAME);
        dorisConfig.setDatabase(schema);
        dorisConfig.setTable(tableName);

        String url = config.get(DorisOptions.URL);
        List<String> feNodes = config.get(DorisOptions.FENODES);

        dorisConfig.setUrl(url);
        dorisConfig.setFeNodes(feNodes);

        if (config.get(DorisOptions.USERNAME) != null) {
            dorisConfig.setUsername(config.get(DorisOptions.USERNAME));
        }

        if (config.get(DorisOptions.PASSWORD) != null) {
            dorisConfig.setPassword(config.get(DorisOptions.PASSWORD));
        }

        LoadConfig loadConfig = getLoadConfig(config);
        dorisConfig.setLoadConfig(loadConfig);
        dorisConfig.setLoadProperties(new Properties());
        dorisConfig.setMaxRetries(config.get(DorisOptions.MAX_RETRIES));
        dorisConfig.setMode(config.get(DorisOptions.WRITE_MODE));
        dorisConfig.setBatchSize(config.get(DorisOptions.BATCH_SIZE));

        return dorisConfig;
    }

    private static LoadConfig getLoadConfig(ReadableConfig config) {
        return LoadConfig.builder()
                .requestTabletSize(config.get(DorisOptions.REQUEST_TABLET_SIZE))
                .requestConnectTimeoutMs(config.get(DorisOptions.REQUEST_CONNECT_TIMEOUT_MS))
                .requestReadTimeoutMs(config.get(DorisOptions.REQUEST_READ_TIMEOUT_MS))
                .requestQueryTimeoutS(config.get(DorisOptions.REQUEST_QUERY_TIMEOUT_SEC))
                .requestRetries(config.get(DorisOptions.REQUEST_RETRIES))
                .requestBatchSize(config.get(DorisOptions.REQUEST_BATCH_SIZE))
                .execMemLimit(config.get(DorisOptions.EXEC_MEM_LIMIT))
                .deserializeQueueSize(config.get(DorisOptions.DESERIALIZE_QUEUE_SIZE))
                .deserializeArrowAsync(config.get(DorisOptions.DESERIALIZE_ARROW_ASYNC))
                .build();
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    protected JdbcDialect getDialect() {
        return new MysqlDialect();
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return new HashSet<>();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = super.optionalOptions();
        Set<ConfigOption<?>> requiredOptions = super.requiredOptions();

        Set<ConfigOption<?>> optionalOptions =
                Stream.of(
                                DorisOptions.USERNAME,
                                DorisOptions.PASSWORD,
                                DorisOptions.FENODES,
                                DorisOptions.TABLE_IDENTIFY,
                                DorisOptions.REQUEST_TABLET_SIZE,
                                DorisOptions.REQUEST_CONNECT_TIMEOUT_MS,
                                DorisOptions.REQUEST_READ_TIMEOUT_MS,
                                DorisOptions.REQUEST_QUERY_TIMEOUT_SEC,
                                DorisOptions.REQUEST_RETRIES,
                                DorisOptions.REQUEST_BATCH_SIZE,
                                DorisOptions.EXEC_MEM_LIMIT,
                                DorisOptions.DESERIALIZE_QUEUE_SIZE,
                                DorisOptions.DESERIALIZE_ARROW_ASYNC,
                                DorisOptions.FIELD_DELIMITER,
                                DorisOptions.LINE_DELIMITER,
                                DorisOptions.MAX_RETRIES,
                                DorisOptions.WRITE_MODE,
                                DorisOptions.BATCH_SIZE)
                        .collect(Collectors.toSet());

        options.addAll(optionalOptions);
        options.addAll(requiredOptions);
        return options;
    }

    @Override
    protected JdbcInputFormatBuilder getInputFormatBuilder() {
        return new DorisInputFormatBuilder(new DorisInputFormat());
    }
}
