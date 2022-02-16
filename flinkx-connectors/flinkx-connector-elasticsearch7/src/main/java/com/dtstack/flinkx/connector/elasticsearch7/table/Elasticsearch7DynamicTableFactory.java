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

package com.dtstack.flinkx.connector.elasticsearch7.table;

import com.dtstack.flinkx.connector.elasticsearch.table.ElasticsearchDynamicTableFactoryBase;
import com.dtstack.flinkx.connector.elasticsearch7.ElasticsearchConf;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.utils.TableSchemaUtils;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.dtstack.flinkx.connector.elasticsearch7.table.Elasticsearch7Options.CLIENT_CONNECT_TIMEOUT_OPTION;
import static com.dtstack.flinkx.connector.elasticsearch7.table.Elasticsearch7Options.CLIENT_KEEPALIVE_TIME_OPTION;
import static com.dtstack.flinkx.connector.elasticsearch7.table.Elasticsearch7Options.CLIENT_MAX_CONNECTION_PER_ROUTE_OPTION;
import static com.dtstack.flinkx.connector.elasticsearch7.table.Elasticsearch7Options.CLIENT_REQUEST_TIMEOUT_OPTION;
import static com.dtstack.flinkx.connector.elasticsearch7.table.Elasticsearch7Options.CLIENT_SOCKET_TIMEOUT_OPTION;
import static com.dtstack.flinkx.lookup.options.LookupOptions.LOOKUP_ASYNC_TIMEOUT;
import static com.dtstack.flinkx.lookup.options.LookupOptions.LOOKUP_CACHE_MAX_ROWS;
import static com.dtstack.flinkx.lookup.options.LookupOptions.LOOKUP_CACHE_PERIOD;
import static com.dtstack.flinkx.lookup.options.LookupOptions.LOOKUP_CACHE_TTL;
import static com.dtstack.flinkx.lookup.options.LookupOptions.LOOKUP_CACHE_TYPE;
import static com.dtstack.flinkx.lookup.options.LookupOptions.LOOKUP_ERROR_LIMIT;
import static com.dtstack.flinkx.lookup.options.LookupOptions.LOOKUP_FETCH_SIZE;
import static com.dtstack.flinkx.lookup.options.LookupOptions.LOOKUP_MAX_RETRIES;
import static com.dtstack.flinkx.lookup.options.LookupOptions.LOOKUP_PARALLELISM;
import static com.dtstack.flinkx.table.options.SinkOptions.SINK_PARALLELISM;
import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchOptions.BULK_FLASH_MAX_SIZE_OPTION;
import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchOptions.BULK_FLUSH_BACKOFF_DELAY_OPTION;
import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchOptions.BULK_FLUSH_BACKOFF_MAX_RETRIES_OPTION;
import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchOptions.BULK_FLUSH_BACKOFF_TYPE_OPTION;
import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchOptions.BULK_FLUSH_INTERVAL_OPTION;
import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchOptions.BULK_FLUSH_MAX_ACTIONS_OPTION;
import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchOptions.CONNECTION_MAX_RETRY_TIMEOUT_OPTION;
import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchOptions.CONNECTION_PATH_PREFIX;
import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchOptions.DOCUMENT_TYPE_OPTION;
import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchOptions.FAILURE_HANDLER_OPTION;
import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchOptions.FLUSH_ON_CHECKPOINT_OPTION;
import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchOptions.FORMAT_OPTION;
import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchOptions.HOSTS_OPTION;
import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchOptions.INDEX_OPTION;
import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchOptions.KEY_DELIMITER_OPTION;
import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchOptions.PASSWORD_OPTION;
import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchOptions.USERNAME_OPTION;

/**
 * @description:
 * @program: flinkx-all
 * @author: lany
 * @create: 2021/06/27 17:29
 */
public class Elasticsearch7DynamicTableFactory extends ElasticsearchDynamicTableFactoryBase {

    private static final String FACTORY_IDENTIFIER = "elasticsearch7-x";

    public Elasticsearch7DynamicTableFactory() {
        super(FACTORY_IDENTIFIER);
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        // 1.所有的requiredOptions和optionalOptions参数
        final ReadableConfig config = helper.getOptions();

        // 2.参数校验
        helper.validate();

        // 3.封装参数
        TableSchema physicalSchema =
                TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());

        return new ElasticsearchDynamicTableSink(
                physicalSchema, getElasticsearchConf(config, physicalSchema));
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        // 1.所有的requiredOptions和optionalOptions参数
        final ReadableConfig config = helper.getOptions();

        // 2.参数校验
        helper.validate();

        // 3.封装参数
        TableSchema physicalSchema =
                TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());

        return new ElasticsearchDynamicTableSource(
                physicalSchema,
                getElasticsearchConf(config, physicalSchema),
                getElasticsearchLookupConf(config, context.getObjectIdentifier().getObjectName()));
    }

    private ElasticsearchConf getElasticsearchConf(
            ReadableConfig readableConfig, TableSchema schema) {
        ElasticsearchConf elasticsearchConf = new ElasticsearchConf();

        elasticsearchConf.setHosts(readableConfig.get(HOSTS_OPTION));
        elasticsearchConf.setIndex(readableConfig.get(INDEX_OPTION));
        elasticsearchConf.setType(readableConfig.get(DOCUMENT_TYPE_OPTION));
        elasticsearchConf.setKeyDelimiter(readableConfig.get(KEY_DELIMITER_OPTION));
        elasticsearchConf.setBatchSize(readableConfig.get(BULK_FLUSH_MAX_ACTIONS_OPTION));
        elasticsearchConf.setParallelism(readableConfig.get(SINK_PARALLELISM));

        elasticsearchConf.setUsername(readableConfig.get(USERNAME_OPTION));
        elasticsearchConf.setPassword(readableConfig.get(PASSWORD_OPTION));

        elasticsearchConf.setConnectTimeout(readableConfig.get(CLIENT_CONNECT_TIMEOUT_OPTION));
        elasticsearchConf.setSocketTimeout(readableConfig.get(CLIENT_SOCKET_TIMEOUT_OPTION));
        elasticsearchConf.setKeepAliveTime(readableConfig.get(CLIENT_KEEPALIVE_TIME_OPTION));
        elasticsearchConf.setMaxConnPerRoute(
                readableConfig.get(CLIENT_MAX_CONNECTION_PER_ROUTE_OPTION));
        elasticsearchConf.setRequestTimeout(readableConfig.get(CLIENT_REQUEST_TIMEOUT_OPTION));

        List<String> keyFields = schema.getPrimaryKey().map(pk -> pk.getColumns()).orElse(null);
        elasticsearchConf.setIds(keyFields);
        return elasticsearchConf;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Stream.of(
                        KEY_DELIMITER_OPTION,
                        FAILURE_HANDLER_OPTION,
                        FLUSH_ON_CHECKPOINT_OPTION,
                        BULK_FLASH_MAX_SIZE_OPTION,
                        BULK_FLUSH_MAX_ACTIONS_OPTION,
                        BULK_FLUSH_INTERVAL_OPTION,
                        BULK_FLUSH_BACKOFF_TYPE_OPTION,
                        BULK_FLUSH_BACKOFF_MAX_RETRIES_OPTION,
                        BULK_FLUSH_BACKOFF_DELAY_OPTION,
                        CONNECTION_MAX_RETRY_TIMEOUT_OPTION,
                        CONNECTION_PATH_PREFIX,
                        FORMAT_OPTION,
                        PASSWORD_OPTION,
                        USERNAME_OPTION,
                        SINK_PARALLELISM,
                        CLIENT_CONNECT_TIMEOUT_OPTION,
                        CLIENT_SOCKET_TIMEOUT_OPTION,
                        CLIENT_KEEPALIVE_TIME_OPTION,
                        CLIENT_REQUEST_TIMEOUT_OPTION,
                        CLIENT_MAX_CONNECTION_PER_ROUTE_OPTION,
                        LOOKUP_CACHE_PERIOD,
                        LOOKUP_CACHE_MAX_ROWS,
                        LOOKUP_CACHE_TTL,
                        LOOKUP_CACHE_TYPE,
                        LOOKUP_MAX_RETRIES,
                        LOOKUP_ERROR_LIMIT,
                        LOOKUP_FETCH_SIZE,
                        LOOKUP_ASYNC_TIMEOUT,
                        LOOKUP_PARALLELISM)
                .collect(Collectors.toSet());
    }
}
