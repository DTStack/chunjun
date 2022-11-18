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

package com.dtstack.chunjun.connector.elasticsearch.table;

import com.dtstack.chunjun.lookup.config.LookupConfig;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_ASYNC_TIMEOUT;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_CACHE_MAX_ROWS;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_CACHE_PERIOD;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_CACHE_TTL;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_CACHE_TYPE;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_ERROR_LIMIT;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_FETCH_SIZE;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_MAX_RETRIES;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_PARALLELISM;
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
import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchConnectorOptions.FAILURE_HANDLER_OPTION;
import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchConnectorOptions.FLUSH_ON_CHECKPOINT_OPTION;
import static org.apache.flink.util.Preconditions.checkNotNull;

public abstract class ElasticsearchDynamicTableFactoryBase
        implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    private final String factoryIdentifier;

    public ElasticsearchDynamicTableFactoryBase(String factoryIdentifier) {
        this.factoryIdentifier = checkNotNull(factoryIdentifier);
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Stream.of(HOSTS_OPTION, INDEX_OPTION).collect(Collectors.toSet());
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        // Inspired by Flink 1.16
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

    @Override
    public String factoryIdentifier() {
        return factoryIdentifier;
    }

    protected LookupConfig getElasticsearchLookupConfig(
            ReadableConfig readableConfig, String tableName) {
        return LookupConfig.build()
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
}
