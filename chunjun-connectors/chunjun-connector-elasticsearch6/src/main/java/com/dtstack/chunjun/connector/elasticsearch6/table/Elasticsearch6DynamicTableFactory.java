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

package com.dtstack.chunjun.connector.elasticsearch6.table;

import com.dtstack.chunjun.connector.elasticsearch.table.ElasticsearchDynamicTableFactoryBase;
import com.dtstack.chunjun.connector.elasticsearch6.Elasticsearch6Config;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.connector.elasticsearch.table.ElasticsearchConnectorOptions.BULK_FLUSH_MAX_ACTIONS_OPTION;
import static org.apache.flink.connector.elasticsearch.table.ElasticsearchConnectorOptions.HOSTS_OPTION;
import static org.apache.flink.connector.elasticsearch.table.ElasticsearchConnectorOptions.INDEX_OPTION;
import static org.apache.flink.connector.elasticsearch.table.ElasticsearchConnectorOptions.KEY_DELIMITER_OPTION;
import static org.apache.flink.connector.elasticsearch.table.ElasticsearchConnectorOptions.PASSWORD_OPTION;
import static org.apache.flink.connector.elasticsearch.table.ElasticsearchConnectorOptions.USERNAME_OPTION;
import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchConnectorOptions.DOCUMENT_TYPE_OPTION;

public class Elasticsearch6DynamicTableFactory extends ElasticsearchDynamicTableFactoryBase {

    private static final String FACTORY_IDENTIFIER = "elasticsearch6-x";

    public Elasticsearch6DynamicTableFactory() {
        super(FACTORY_IDENTIFIER);
    }

    private static final Set<ConfigOption<?>> requiredOptions =
            Stream.of(HOSTS_OPTION, INDEX_OPTION, DOCUMENT_TYPE_OPTION).collect(Collectors.toSet());

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

        return new Elasticsearch6DynamicTableSink(
                resolvedSchema, getElasticsearchConfig(config, resolvedSchema));
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

        return new Elasticsearch6DynamicTableSource(
                resolvedSchema,
                getElasticsearchConfig(config, resolvedSchema),
                getElasticsearchLookupConfig(
                        config, context.getObjectIdentifier().getObjectName()));
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> requiredOptions = super.requiredOptions();
        requiredOptions.add(DOCUMENT_TYPE_OPTION);
        return requiredOptions;
    }

    private Elasticsearch6Config getElasticsearchConfig(
            ReadableConfig readableConfig, ResolvedSchema schema) {
        Elasticsearch6Config elasticsearchConfig = new Elasticsearch6Config();

        elasticsearchConfig.setHosts(readableConfig.get(HOSTS_OPTION));
        elasticsearchConfig.setIndex(readableConfig.get(INDEX_OPTION));
        elasticsearchConfig.setType(readableConfig.get(DOCUMENT_TYPE_OPTION));
        elasticsearchConfig.setKeyDelimiter(readableConfig.get(KEY_DELIMITER_OPTION));
        elasticsearchConfig.setBatchSize(readableConfig.get(BULK_FLUSH_MAX_ACTIONS_OPTION));
        elasticsearchConfig.setUsername(readableConfig.get(USERNAME_OPTION));
        elasticsearchConfig.setPassword(readableConfig.get(PASSWORD_OPTION));

        List<String> keyFields =
                schema.getPrimaryKey()
                        .map(UniqueConstraint::getColumns)
                        .orElse(Collections.emptyList());
        elasticsearchConfig.setIds(keyFields);
        return elasticsearchConfig;
    }
}
