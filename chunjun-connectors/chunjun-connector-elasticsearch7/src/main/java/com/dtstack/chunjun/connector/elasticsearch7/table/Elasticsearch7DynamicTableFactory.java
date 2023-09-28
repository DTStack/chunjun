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

package com.dtstack.chunjun.connector.elasticsearch7.table;

import com.dtstack.chunjun.connector.elasticsearch.table.ElasticsearchDynamicTableFactoryBase;
import com.dtstack.chunjun.connector.elasticsearch7.ElasticsearchConfig;
import com.dtstack.chunjun.connector.elasticsearch7.SslConfig;
import com.dtstack.chunjun.util.JsonUtil;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.FactoryUtil;

import org.apache.commons.lang3.StringUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.dtstack.chunjun.connector.elasticsearch7.table.Elasticsearch7Options.CLIENT_CONNECT_TIMEOUT_OPTION;
import static com.dtstack.chunjun.connector.elasticsearch7.table.Elasticsearch7Options.CLIENT_KEEPALIVE_TIME_OPTION;
import static com.dtstack.chunjun.connector.elasticsearch7.table.Elasticsearch7Options.CLIENT_MAX_CONNECTION_PER_ROUTE_OPTION;
import static com.dtstack.chunjun.connector.elasticsearch7.table.Elasticsearch7Options.CLIENT_REQUEST_TIMEOUT_OPTION;
import static com.dtstack.chunjun.connector.elasticsearch7.table.Elasticsearch7Options.CLIENT_SOCKET_TIMEOUT_OPTION;
import static com.dtstack.chunjun.connector.elasticsearch7.table.Elasticsearch7Options.SEARCH_QUERY;
import static com.dtstack.chunjun.connector.elasticsearch7.table.Elasticsearch7Options.WRITE_MODE;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_ASYNC_TIMEOUT;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_CACHE_MAX_ROWS;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_CACHE_PERIOD;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_CACHE_TTL;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_CACHE_TYPE;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_ERROR_LIMIT;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_FETCH_SIZE;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_MAX_RETRIES;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_PARALLELISM;
import static com.dtstack.chunjun.security.SslOptions.KEYSTOREFILENAME;
import static com.dtstack.chunjun.security.SslOptions.KEYSTOREPASS;
import static com.dtstack.chunjun.security.SslOptions.TYPE;
import static com.dtstack.chunjun.table.options.SinkOptions.SINK_BUFFER_FLUSH_MAX_ROWS;
import static com.dtstack.chunjun.table.options.SinkOptions.SINK_PARALLELISM;
import static org.apache.flink.connector.elasticsearch.table.ElasticsearchConnectorOptions.BULK_FLUSH_BACKOFF_DELAY_OPTION;
import static org.apache.flink.connector.elasticsearch.table.ElasticsearchConnectorOptions.BULK_FLUSH_BACKOFF_MAX_RETRIES_OPTION;
import static org.apache.flink.connector.elasticsearch.table.ElasticsearchConnectorOptions.BULK_FLUSH_BACKOFF_TYPE_OPTION;
import static org.apache.flink.connector.elasticsearch.table.ElasticsearchConnectorOptions.BULK_FLUSH_INTERVAL_OPTION;
import static org.apache.flink.connector.elasticsearch.table.ElasticsearchConnectorOptions.BULK_FLUSH_MAX_ACTIONS_OPTION;
import static org.apache.flink.connector.elasticsearch.table.ElasticsearchConnectorOptions.FORMAT_OPTION;
import static org.apache.flink.connector.elasticsearch.table.ElasticsearchConnectorOptions.HOSTS_OPTION;
import static org.apache.flink.connector.elasticsearch.table.ElasticsearchConnectorOptions.INDEX_OPTION;
import static org.apache.flink.connector.elasticsearch.table.ElasticsearchConnectorOptions.KEY_DELIMITER_OPTION;
import static org.apache.flink.connector.elasticsearch.table.ElasticsearchConnectorOptions.PASSWORD_OPTION;
import static org.apache.flink.connector.elasticsearch.table.ElasticsearchConnectorOptions.USERNAME_OPTION;
import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchConnectorOptions.BULK_FLASH_MAX_SIZE_OPTION;
import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchConnectorOptions.CONNECTION_PATH_PREFIX;
import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchConnectorOptions.DOCUMENT_TYPE_OPTION;
import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchConnectorOptions.FAILURE_HANDLER_OPTION;
import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchConnectorOptions.FLUSH_ON_CHECKPOINT_OPTION;

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
        ResolvedSchema resolvedSchema = context.getCatalogTable().getResolvedSchema();

        ElasticsearchConfig elasticsearchConfig = getElasticsearchConfig(config, resolvedSchema);
        return new ElasticsearchDynamicTableSink(resolvedSchema, elasticsearchConfig);
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
        ResolvedSchema resolvedSchema = context.getCatalogTable().getResolvedSchema();

        ElasticsearchConfig elasticsearchConfig = getElasticsearchConfig(config, resolvedSchema);
        return new ElasticsearchDynamicTableSource(
                resolvedSchema,
                elasticsearchConfig,
                getElasticsearchLookupConfig(
                        config, context.getObjectIdentifier().getObjectName()));
    }

    private ElasticsearchConfig getElasticsearchConfig(
            ReadableConfig readableConfig, ResolvedSchema schema) {
        ElasticsearchConfig elasticsearchConfig = new ElasticsearchConfig();

        elasticsearchConfig.setHosts(readableConfig.get(HOSTS_OPTION));
        elasticsearchConfig.setIndex(readableConfig.get(INDEX_OPTION));
        elasticsearchConfig.setType(readableConfig.get(DOCUMENT_TYPE_OPTION));
        elasticsearchConfig.setKeyDelimiter(readableConfig.get(KEY_DELIMITER_OPTION));
        elasticsearchConfig.setBatchSize(readableConfig.get(BULK_FLUSH_MAX_ACTIONS_OPTION));
        elasticsearchConfig.setParallelism(readableConfig.get(SINK_PARALLELISM));

        elasticsearchConfig.setUsername(readableConfig.get(USERNAME_OPTION));
        elasticsearchConfig.setPassword(readableConfig.get(PASSWORD_OPTION));

        elasticsearchConfig.setConnectTimeout(readableConfig.get(CLIENT_CONNECT_TIMEOUT_OPTION));
        elasticsearchConfig.setSocketTimeout(readableConfig.get(CLIENT_SOCKET_TIMEOUT_OPTION));
        elasticsearchConfig.setKeepAliveTime(readableConfig.get(CLIENT_KEEPALIVE_TIME_OPTION));
        elasticsearchConfig.setMaxConnPerRoute(
                readableConfig.get(CLIENT_MAX_CONNECTION_PER_ROUTE_OPTION));
        elasticsearchConfig.setRequestTimeout(readableConfig.get(CLIENT_REQUEST_TIMEOUT_OPTION));

        List<String> keyFields =
                schema.getPrimaryKey()
                        .map(UniqueConstraint::getColumns)
                        .orElse(Collections.emptyList());
        elasticsearchConfig.setIds(keyFields);

        String filename = readableConfig.get(KEYSTOREFILENAME);
        if (StringUtils.isNotBlank(filename)) {
            SslConfig sslConfig = new SslConfig();
            sslConfig.setUseLocalFile(true);
            sslConfig.setFileName(filename);

            String pass = readableConfig.get(KEYSTOREPASS);
            if (StringUtils.isNotBlank(pass)) {
                sslConfig.setKeyStorePass(pass);
            }
            String type = readableConfig.get(TYPE);
            if (StringUtils.isNotBlank(type)) {
                sslConfig.setType(type);
            }

            elasticsearchConfig.setSslConfig(sslConfig);
        }

        String searchQuery = readableConfig.get(SEARCH_QUERY);
        if (StringUtils.isNotBlank(searchQuery)) {
            try {
                Map<String, Object> query =
                        JsonUtil.objectMapper.readValue(searchQuery, HashMap.class);
                elasticsearchConfig.setQuery(query);
            } catch (Exception e) {
                throw new RuntimeException("error parse [" + searchQuery + "] to json", e);
            }
        }
        elasticsearchConfig.setWriteMode(readableConfig.get(WRITE_MODE));

        return elasticsearchConfig;
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
                        LOOKUP_PARALLELISM,
                        KEYSTOREFILENAME,
                        KEYSTOREPASS,
                        TYPE,
                        SEARCH_QUERY,
                        WRITE_MODE,
                        SINK_BUFFER_FLUSH_MAX_ROWS)
                .collect(Collectors.toSet());
    }
}
