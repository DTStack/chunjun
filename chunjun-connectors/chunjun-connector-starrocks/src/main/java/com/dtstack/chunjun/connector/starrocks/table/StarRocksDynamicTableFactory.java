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
package com.dtstack.chunjun.connector.starrocks.table;

import com.dtstack.chunjun.connector.starrocks.conf.LoadConf;
import com.dtstack.chunjun.connector.starrocks.conf.LoadConfBuilder;
import com.dtstack.chunjun.connector.starrocks.conf.StarRocksConf;
import com.dtstack.chunjun.connector.starrocks.sink.StarRocksDynamicTableSink;
import com.dtstack.chunjun.connector.starrocks.source.StarRocksDynamicTableSource;
import com.dtstack.chunjun.lookup.conf.LookupConf;
import com.dtstack.chunjun.lookup.conf.LookupConfFactory;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.utils.TableSchemaUtils;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.dtstack.chunjun.connector.starrocks.options.StarRocksCommonOptions.FENODES;
import static com.dtstack.chunjun.connector.starrocks.options.StarRocksCommonOptions.MAX_RETRIES;
import static com.dtstack.chunjun.connector.starrocks.options.StarRocksCommonOptions.PASSWORD;
import static com.dtstack.chunjun.connector.starrocks.options.StarRocksCommonOptions.SCHEMA_NAME;
import static com.dtstack.chunjun.connector.starrocks.options.StarRocksCommonOptions.TABLE_NAME;
import static com.dtstack.chunjun.connector.starrocks.options.StarRocksCommonOptions.URL;
import static com.dtstack.chunjun.connector.starrocks.options.StarRocksCommonOptions.USERNAME;
import static com.dtstack.chunjun.connector.starrocks.options.StarRocksSinkOptions.NAME_MAPPED;
import static com.dtstack.chunjun.connector.starrocks.options.StarRocksSinkOptions.SINK_BUFFER_FLUSH_MAX_ROWS;
import static com.dtstack.chunjun.connector.starrocks.options.StarRocksSinkOptions.SINK_SEMANTIC;
import static com.dtstack.chunjun.connector.starrocks.options.StarRocksSourceOptions.FILTER_STATEMENT;
import static com.dtstack.chunjun.connector.starrocks.options.StarRocksSourceOptions.SCAN_BE_CLIENT_KEEP_LIVE_MIN;
import static com.dtstack.chunjun.connector.starrocks.options.StarRocksSourceOptions.SCAN_BE_CLIENT_TIMEOUT;
import static com.dtstack.chunjun.connector.starrocks.options.StarRocksSourceOptions.SCAN_BE_FETCH_BYTES_LIMIT;
import static com.dtstack.chunjun.connector.starrocks.options.StarRocksSourceOptions.SCAN_BE_FETCH_ROWS;
import static com.dtstack.chunjun.connector.starrocks.options.StarRocksSourceOptions.SCAN_BE_PARAM_PROPERTIES;
import static com.dtstack.chunjun.connector.starrocks.options.StarRocksSourceOptions.SCAN_BE_QUERY_TIMEOUT_S;
import static com.dtstack.chunjun.connector.starrocks.options.StreamLoadOptions.HTTP_CHECK_TIMEOUT;
import static com.dtstack.chunjun.connector.starrocks.options.StreamLoadOptions.QUEUE_OFFER_TIMEOUT;
import static com.dtstack.chunjun.connector.starrocks.options.StreamLoadOptions.QUEUE_POLL_TIMEOUT;
import static com.dtstack.chunjun.connector.starrocks.options.StreamLoadOptions.SINK_BATCH_MAX_BYTES;
import static com.dtstack.chunjun.connector.starrocks.options.StreamLoadOptions.SINK_BATCH_MAX_ROWS;
import static com.dtstack.chunjun.connector.starrocks.options.StreamLoadOptions.STREAM_LOAD_HEAD_PROPERTIES;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_ASYNC_TIMEOUT;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_CACHE_MAX_ROWS;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_CACHE_PERIOD;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_CACHE_TTL;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_CACHE_TYPE;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_ERROR_LIMIT;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_FETCH_SIZE;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_MAX_RETRIES;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_PARALLELISM;
import static com.dtstack.chunjun.table.options.SinkOptions.SINK_BUFFER_FLUSH_INTERVAL;

/**
 * @author lihongwei
 * @date 2022/04/11
 */
public class StarRocksDynamicTableFactory
        implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    private static final String IDENTIFIER = "starrocks-x";

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        TableSchema physicalSchema =
                TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());

        return new StarRocksDynamicTableSource(
                createSourceConfByOptions(helper.getOptions(), physicalSchema),
                createLookupConfByOptions(helper.getOptions()),
                physicalSchema);
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        ReadableConfig options = helper.getOptions();
        StarRocksConf sinkConf = createSinkConfByOptions(options);
        TableSchema physicalSchema =
                TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());
        return new StarRocksDynamicTableSink(sinkConf, physicalSchema);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    private StarRocksConf createSourceConfByOptions(
            ReadableConfig options, TableSchema tableSchema) {
        StarRocksConf sourceConf = createCommonConfByOptions(options);
        // source options
        String filterStatement = options.get(FILTER_STATEMENT);
        Integer beClientKeepLiveMin = options.get(SCAN_BE_CLIENT_KEEP_LIVE_MIN);
        Integer beQueryTimeoutSecond = options.get(SCAN_BE_QUERY_TIMEOUT_S);
        Integer beClientTimeout = options.get(SCAN_BE_CLIENT_TIMEOUT);
        Integer beFetchRows = options.get(SCAN_BE_FETCH_ROWS);
        Long beFetchMaxBytes = options.get(SCAN_BE_FETCH_BYTES_LIMIT);
        Map<String, String> beSocketProperties = options.get(SCAN_BE_PARAM_PROPERTIES);

        // loading
        sourceConf.setFilterStatement(filterStatement);
        sourceConf.setBeClientKeepLiveMin(beClientKeepLiveMin);
        sourceConf.setBeQueryTimeoutSecond(beQueryTimeoutSecond);
        sourceConf.setBeClientTimeout(beClientTimeout);
        sourceConf.setBeFetchRows(beFetchRows);
        sourceConf.setBeFetchMaxBytes(beFetchMaxBytes);
        sourceConf.setBeSocketProperties(beSocketProperties);

        sourceConf.setFieldNames(tableSchema.getFieldNames());
        sourceConf.setDataTypes(tableSchema.getFieldDataTypes());

        return sourceConf;
    }

    private LookupConf createLookupConfByOptions(ReadableConfig options) {
        return LookupConfFactory.createLookupConf(options);
    }

    private StarRocksConf createSinkConfByOptions(ReadableConfig options) {
        StarRocksConf sinkConf = createCommonConfByOptions(options);
        // sink options
        boolean nameMapped = options.get(NAME_MAPPED);
        Integer batchSize = options.get(SINK_BUFFER_FLUSH_MAX_ROWS);
        Long sinkInternal = options.get(SINK_BUFFER_FLUSH_INTERVAL);
        LoadConf loadConf = getLoadConf(options);
        // loading
        sinkConf.setNameMapped(nameMapped);
        sinkConf.setBatchSize(batchSize);
        sinkConf.setFlushIntervalMills(sinkInternal);
        sinkConf.setLoadConf(loadConf);
        return sinkConf;
    }

    private LoadConf getLoadConf(ReadableConfig options) {
        LoadConfBuilder loadConfBuilder = new LoadConfBuilder();
        return loadConfBuilder
                .setBatchMaxSize(options.get(SINK_BATCH_MAX_BYTES))
                .setBatchMaxRows(options.get(SINK_BATCH_MAX_ROWS))
                .setHttpCheckTimeoutMs(options.get(HTTP_CHECK_TIMEOUT))
                .setQueueOfferTimeoutMs(options.get(QUEUE_OFFER_TIMEOUT))
                .setQueuePollTimeoutMs(options.get(QUEUE_POLL_TIMEOUT))
                .setHeadProperties(options.get(STREAM_LOAD_HEAD_PROPERTIES))
                .build();
    }

    protected StarRocksConf createCommonConfByOptions(ReadableConfig options) {
        StarRocksConf conf = new StarRocksConf();
        // common options
        String url = options.get(URL);
        List<String> feNodes = options.get(FENODES);
        String database = options.get(SCHEMA_NAME);
        String tableName = options.get(TABLE_NAME);
        String username = options.get(USERNAME);
        String password = options.get(PASSWORD);
        Integer maxRetries = options.get(MAX_RETRIES);
        // loading
        conf.setUrl(url);
        conf.setFeNodes(feNodes);
        conf.setDatabase(database);
        conf.setTable(tableName);
        conf.setUsername(username);
        conf.setPassword(password);
        conf.setMaxRetries(maxRetries);
        return conf;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> requiredOptions = new HashSet<>();
        requiredOptions.add(URL);
        requiredOptions.add(FENODES);
        requiredOptions.add(SCHEMA_NAME);
        requiredOptions.add(TABLE_NAME);
        requiredOptions.add(USERNAME);
        requiredOptions.add(PASSWORD);
        return requiredOptions;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> optionalOptions = new HashSet<>();

        // common
        optionalOptions.add(MAX_RETRIES);

        // source
        optionalOptions.add(FILTER_STATEMENT);
        optionalOptions.add(SCAN_BE_CLIENT_KEEP_LIVE_MIN);
        optionalOptions.add(SCAN_BE_QUERY_TIMEOUT_S);
        optionalOptions.add(SCAN_BE_CLIENT_TIMEOUT);
        optionalOptions.add(SCAN_BE_FETCH_ROWS);
        optionalOptions.add(SCAN_BE_FETCH_BYTES_LIMIT);
        optionalOptions.add(SCAN_BE_PARAM_PROPERTIES);

        // lookup
        optionalOptions.add(LOOKUP_CACHE_PERIOD);
        optionalOptions.add(LOOKUP_CACHE_MAX_ROWS);
        optionalOptions.add(LOOKUP_CACHE_TTL);
        optionalOptions.add(LOOKUP_CACHE_TYPE);
        optionalOptions.add(LOOKUP_MAX_RETRIES);
        optionalOptions.add(LOOKUP_ERROR_LIMIT);
        optionalOptions.add(LOOKUP_FETCH_SIZE);
        optionalOptions.add(LOOKUP_ASYNC_TIMEOUT);
        optionalOptions.add(LOOKUP_PARALLELISM);

        // sink
        optionalOptions.add(NAME_MAPPED);
        optionalOptions.add(SINK_BUFFER_FLUSH_MAX_ROWS);
        optionalOptions.add(SINK_BUFFER_FLUSH_INTERVAL);
        optionalOptions.add(SINK_SEMANTIC);

        // stream load
        optionalOptions.add(SINK_BATCH_MAX_ROWS);
        optionalOptions.add(SINK_BATCH_MAX_BYTES);
        optionalOptions.add(HTTP_CHECK_TIMEOUT);
        optionalOptions.add(QUEUE_OFFER_TIMEOUT);
        optionalOptions.add(QUEUE_POLL_TIMEOUT);
        optionalOptions.add(STREAM_LOAD_HEAD_PROPERTIES);
        return optionalOptions;
    }
}
