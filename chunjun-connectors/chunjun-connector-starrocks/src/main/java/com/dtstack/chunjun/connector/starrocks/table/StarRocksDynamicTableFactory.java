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
import com.dtstack.chunjun.connector.starrocks.options.ConstantValue;
import com.dtstack.chunjun.connector.starrocks.options.StarRocksCommonOptions;
import com.dtstack.chunjun.connector.starrocks.options.StarRocksSinkOptions;
import com.dtstack.chunjun.connector.starrocks.options.StarRocksSourceOptions;
import com.dtstack.chunjun.connector.starrocks.options.StreamLoadOptions;
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

import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_ASYNC_TIMEOUT;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_CACHE_MAX_ROWS;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_CACHE_PERIOD;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_CACHE_TTL;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_CACHE_TYPE;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_ERROR_LIMIT;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_FETCH_SIZE;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_MAX_RETRIES;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_PARALLELISM;

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
        helper.validateExcept(ConstantValue.SINK_PROPERTIES_PREFIX);
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
        String filterStatement = options.get(StarRocksSourceOptions.FILTER_STATEMENT);
        Integer beClientKeepLiveMin =
                options.get(StarRocksSourceOptions.SCAN_BE_CLIENT_KEEP_LIVE_MIN);
        Integer beQueryTimeoutSecond = options.get(StarRocksSourceOptions.SCAN_BE_QUERY_TIMEOUT_S);
        Integer beClientTimeout = options.get(StarRocksSourceOptions.SCAN_BE_CLIENT_TIMEOUT);
        Integer beFetchRows = options.get(StarRocksSourceOptions.SCAN_BE_FETCH_ROWS);
        Long beFetchMaxBytes = options.get(StarRocksSourceOptions.SCAN_BE_FETCH_BYTES_LIMIT);
        Map<String, String> beSocketProperties =
                options.get(StarRocksSourceOptions.SCAN_BE_PARAM_PROPERTIES);

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
        boolean nameMapped = options.get(StarRocksSinkOptions.NAME_MAPPED);
        Integer batchSize = options.get(StarRocksSinkOptions.BATCH_SIZE);
        LoadConf loadConf = getLoadConf(options);
        // loading
        sinkConf.setNameMapped(nameMapped);
        sinkConf.setBatchSize(batchSize);
        sinkConf.setLoadConf(loadConf);
        return sinkConf;
    }

    private LoadConf getLoadConf(ReadableConfig options) {
        LoadConfBuilder loadConfBuilder = new LoadConfBuilder();
        return loadConfBuilder
                .setBatchMaxSize(options.get(StreamLoadOptions.SINK_BATCH_MAX_BYTES))
                .setBatchMaxRows(options.get(StreamLoadOptions.SINK_BATCH_MAX_ROWS))
                .setHttpCheckTimeoutMs(options.get(StreamLoadOptions.HTTP_CHECK_TIMEOUT))
                .setQueueOfferTimeoutMs(options.get(StreamLoadOptions.QUEUE_OFFER_TIMEOUT))
                .setQueuePollTimeoutMs(options.get(StreamLoadOptions.QUEUE_POLL_TIMEOUT))
                .setHeadProperties(options.get(StreamLoadOptions.STREAM_LOAD_HEAD_PROPERTIES))
                .build();
    }

    protected StarRocksConf createCommonConfByOptions(ReadableConfig options) {
        StarRocksConf conf = new StarRocksConf();
        // common options
        String url = options.get(StarRocksCommonOptions.URL);
        List<String> feNodes = options.get(StarRocksCommonOptions.FENODES);
        String database = options.get(StarRocksCommonOptions.SCHEMA_NAME);
        String tableName = options.get(StarRocksCommonOptions.TABLE_NAME);
        String username = options.get(StarRocksCommonOptions.USERNAME);
        String password = options.get(StarRocksCommonOptions.PASSWORD);
        Integer maxRetries = options.get(StarRocksCommonOptions.MAX_RETRIES);
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
        requiredOptions.add(StarRocksCommonOptions.URL);
        requiredOptions.add(StarRocksCommonOptions.FENODES);
        requiredOptions.add(StarRocksCommonOptions.SCHEMA_NAME);
        requiredOptions.add(StarRocksCommonOptions.TABLE_NAME);
        requiredOptions.add(StarRocksCommonOptions.USERNAME);
        requiredOptions.add(StarRocksCommonOptions.PASSWORD);
        return requiredOptions;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> optionalOptions = new HashSet<>();

        // common
        optionalOptions.add(StarRocksCommonOptions.MAX_RETRIES);

        // source
        optionalOptions.add(StarRocksSourceOptions.FILTER_STATEMENT);
        optionalOptions.add(StarRocksSourceOptions.SCAN_BE_CLIENT_KEEP_LIVE_MIN);
        optionalOptions.add(StarRocksSourceOptions.SCAN_BE_QUERY_TIMEOUT_S);
        optionalOptions.add(StarRocksSourceOptions.SCAN_BE_CLIENT_TIMEOUT);
        optionalOptions.add(StarRocksSourceOptions.SCAN_BE_FETCH_ROWS);
        optionalOptions.add(StarRocksSourceOptions.SCAN_BE_FETCH_BYTES_LIMIT);
        optionalOptions.add(StarRocksSourceOptions.SCAN_BE_PARAM_PROPERTIES);

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
        optionalOptions.add(StarRocksSinkOptions.NAME_MAPPED);
        optionalOptions.add(StarRocksSinkOptions.BATCH_SIZE);
        optionalOptions.add(StarRocksSinkOptions.SINK_SEMANTIC);

        // stream load
        optionalOptions.add(StreamLoadOptions.SINK_BATCH_MAX_ROWS);
        optionalOptions.add(StreamLoadOptions.SINK_BATCH_MAX_BYTES);
        optionalOptions.add(StreamLoadOptions.HTTP_CHECK_TIMEOUT);
        optionalOptions.add(StreamLoadOptions.QUEUE_OFFER_TIMEOUT);
        optionalOptions.add(StreamLoadOptions.QUEUE_POLL_TIMEOUT);
        optionalOptions.add(StreamLoadOptions.STREAM_LOAD_HEAD_PROPERTIES);
        return optionalOptions;
    }
}
