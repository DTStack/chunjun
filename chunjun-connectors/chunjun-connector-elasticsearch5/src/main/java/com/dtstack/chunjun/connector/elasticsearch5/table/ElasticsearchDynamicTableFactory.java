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

package com.dtstack.chunjun.connector.elasticsearch5.table;

import com.dtstack.chunjun.connector.elasticsearch5.conf.ElasticsearchConf;
import com.dtstack.chunjun.connector.elasticsearch5.sink.ElasticsearchDynamicTableSink;
import com.dtstack.chunjun.connector.elasticsearch5.source.ElasticsearchDynamicTableSource;
import com.dtstack.chunjun.lookup.conf.LookupConf;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.utils.TableSchemaUtils;

import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.dtstack.chunjun.connector.elasticsearch5.options.DtElasticsearchOptions.ACTION_TIMEOUT_OPTION;
import static com.dtstack.chunjun.connector.elasticsearch5.options.DtElasticsearchOptions.CLUSTER_OPTION;
import static com.dtstack.chunjun.connector.elasticsearch5.utils.ElasticsearchConstants.IDENTIFIER;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_ASYNCTIMEOUT;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_CACHE_MAX_ROWS;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_CACHE_PERIOD;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_CACHE_TTL;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_CACHE_TYPE;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_ERRORLIMIT;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_FETCH_SIZE;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_MAX_RETRIES;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_PARALLELISM;
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
 * @create: 2021/06/27 23:58
 */
public class ElasticsearchDynamicTableFactory
        implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    private static final Set<ConfigOption<?>> requiredOptions =
            Stream.of(HOSTS_OPTION, INDEX_OPTION, DOCUMENT_TYPE_OPTION, CLUSTER_OPTION)
                    .collect(Collectors.toSet());
    private static final Set<ConfigOption<?>> optionalOptions =
            Stream.of(
                            ACTION_TIMEOUT_OPTION,
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
                            LOOKUP_CACHE_PERIOD,
                            LOOKUP_CACHE_MAX_ROWS,
                            LOOKUP_CACHE_TTL,
                            LOOKUP_CACHE_TYPE,
                            LOOKUP_MAX_RETRIES,
                            LOOKUP_ERRORLIMIT,
                            LOOKUP_FETCH_SIZE,
                            LOOKUP_ASYNCTIMEOUT,
                            LOOKUP_PARALLELISM)
                    .collect(Collectors.toSet());

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

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return requiredOptions;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return optionalOptions;
    }

    private ElasticsearchConf getElasticsearchConf(
            ReadableConfig readableConfig, TableSchema schema) {
        ElasticsearchConf elasticsearchConf = new ElasticsearchConf();
        boolean isAuthMesh = false;

        elasticsearchConf.setHosts(readableConfig.get(HOSTS_OPTION));
        elasticsearchConf.setIndex(readableConfig.get(INDEX_OPTION));
        elasticsearchConf.setType(readableConfig.get(DOCUMENT_TYPE_OPTION));
        elasticsearchConf.setKeyDelimiter(readableConfig.get(KEY_DELIMITER_OPTION));
        elasticsearchConf.setCluster(readableConfig.get(CLUSTER_OPTION));
        elasticsearchConf.setActionTimeout(readableConfig.get(ACTION_TIMEOUT_OPTION));
        String username = readableConfig.get(USERNAME_OPTION);
        String password = readableConfig.get(PASSWORD_OPTION);
        if (StringUtils.isNotEmpty(username) && StringUtils.isNotEmpty(password)) {
            elasticsearchConf.setUsername(username);
            elasticsearchConf.setPassword(password);
            isAuthMesh = true;
        }
        elasticsearchConf.setAuthMesh(isAuthMesh);

        List<String> keyFields = schema.getPrimaryKey().map(pk -> pk.getColumns()).orElse(null);
        elasticsearchConf.setIds(keyFields);
        return elasticsearchConf;
    }

    private LookupConf getElasticsearchLookupConf(ReadableConfig readableConfig, String tableName) {
        return LookupConf.build()
                .setTableName(tableName)
                .setPeriod(readableConfig.get(LOOKUP_CACHE_PERIOD))
                .setCacheSize(readableConfig.get(LOOKUP_CACHE_MAX_ROWS))
                .setCacheTtl(readableConfig.get(LOOKUP_CACHE_TTL))
                .setCache(readableConfig.get(LOOKUP_CACHE_TYPE))
                .setMaxRetryTimes(readableConfig.get(LOOKUP_MAX_RETRIES))
                .setErrorLimit(readableConfig.get(LOOKUP_ERRORLIMIT))
                .setFetchSize(readableConfig.get(LOOKUP_FETCH_SIZE))
                .setAsyncTimeout(readableConfig.get(LOOKUP_ASYNCTIMEOUT))
                .setParallelism(readableConfig.get(LOOKUP_PARALLELISM));
    }
}
