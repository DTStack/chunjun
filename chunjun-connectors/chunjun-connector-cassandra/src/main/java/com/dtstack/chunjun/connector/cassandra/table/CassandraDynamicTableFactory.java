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

package com.dtstack.chunjun.connector.cassandra.table;

import com.dtstack.chunjun.connector.cassandra.config.CassandraLookupConfig;
import com.dtstack.chunjun.connector.cassandra.config.CassandraSinkConfig;
import com.dtstack.chunjun.connector.cassandra.config.CassandraSourceConfig;
import com.dtstack.chunjun.connector.cassandra.sink.CassandraDynamicTableSink;
import com.dtstack.chunjun.connector.cassandra.source.CassandraDynamicTableSource;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;

import java.util.HashSet;
import java.util.Set;

import static com.dtstack.chunjun.connector.cassandra.optinos.CassandraCommonOptions.ASYNC_WRITE;
import static com.dtstack.chunjun.connector.cassandra.optinos.CassandraCommonOptions.CLUSTER_NAME;
import static com.dtstack.chunjun.connector.cassandra.optinos.CassandraCommonOptions.CONNECT_TIMEOUT_MILLISECONDS;
import static com.dtstack.chunjun.connector.cassandra.optinos.CassandraCommonOptions.CONSISTENCY;
import static com.dtstack.chunjun.connector.cassandra.optinos.CassandraCommonOptions.CORE_CONNECTIONS_PER_HOST;
import static com.dtstack.chunjun.connector.cassandra.optinos.CassandraCommonOptions.HOST;
import static com.dtstack.chunjun.connector.cassandra.optinos.CassandraCommonOptions.HOST_DISTANCE;
import static com.dtstack.chunjun.connector.cassandra.optinos.CassandraCommonOptions.KEY_SPACES;
import static com.dtstack.chunjun.connector.cassandra.optinos.CassandraCommonOptions.MAX_CONNECTIONS__PER_HOST;
import static com.dtstack.chunjun.connector.cassandra.optinos.CassandraCommonOptions.MAX_QUEUE_SIZE;
import static com.dtstack.chunjun.connector.cassandra.optinos.CassandraCommonOptions.MAX_REQUESTS_PER_CONNECTION;
import static com.dtstack.chunjun.connector.cassandra.optinos.CassandraCommonOptions.PASSWORD;
import static com.dtstack.chunjun.connector.cassandra.optinos.CassandraCommonOptions.POOL_TIMEOUT_MILLISECONDS;
import static com.dtstack.chunjun.connector.cassandra.optinos.CassandraCommonOptions.PORT;
import static com.dtstack.chunjun.connector.cassandra.optinos.CassandraCommonOptions.READ_TIME_OUT_MILLISECONDS;
import static com.dtstack.chunjun.connector.cassandra.optinos.CassandraCommonOptions.TABLE_NAME;
import static com.dtstack.chunjun.connector.cassandra.optinos.CassandraCommonOptions.USER_NAME;
import static com.dtstack.chunjun.connector.cassandra.optinos.CassandraCommonOptions.USE_SSL;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_ASYNC_TIMEOUT;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_CACHE_MAX_ROWS;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_CACHE_PERIOD;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_CACHE_TTL;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_CACHE_TYPE;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_ERROR_LIMIT;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_FETCH_SIZE;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_MAX_RETRIES;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_PARALLELISM;
import static com.dtstack.chunjun.source.options.SourceOptions.SCAN_FETCH_SIZE;
import static com.dtstack.chunjun.source.options.SourceOptions.SCAN_INCREMENT_COLUMN;
import static com.dtstack.chunjun.source.options.SourceOptions.SCAN_INCREMENT_COLUMN_TYPE;
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

public class CassandraDynamicTableFactory
        implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    private static final String IDENTIFIED = "cassandra-x";

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
        DataType dataType = context.getPhysicalRowDataType();

        CassandraSinkConfig sinkConf = CassandraSinkConfig.from(config);

        return new CassandraDynamicTableSink(sinkConf, resolvedSchema, dataType);
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);

        final ReadableConfig options = helper.getOptions();

        helper.validate();

        ResolvedSchema resolvedSchema = context.getCatalogTable().getResolvedSchema();
        DataType dataType = context.getPhysicalRowDataType();

        CassandraSourceConfig cassandraSourceConf = CassandraSourceConfig.from(options);
        CassandraLookupConfig cassandraLookupConfig = CassandraLookupConfig.from(options);

        return new CassandraDynamicTableSource(
                cassandraSourceConf, cassandraLookupConfig, resolvedSchema, dataType);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIED;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> requiredConfigOptions = new HashSet<>();
        requiredConfigOptions.add(HOST);
        requiredConfigOptions.add(TABLE_NAME);
        return requiredConfigOptions;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> optionalOptions = new HashSet<>();

        // common
        optionalOptions.add(PORT);
        optionalOptions.add(USER_NAME);
        optionalOptions.add(PASSWORD);
        optionalOptions.add(KEY_SPACES);
        optionalOptions.add(HOST_DISTANCE);
        optionalOptions.add(CLUSTER_NAME);
        optionalOptions.add(CONSISTENCY);

        // sink
        optionalOptions.add(CORE_CONNECTIONS_PER_HOST);
        optionalOptions.add(MAX_CONNECTIONS__PER_HOST);
        optionalOptions.add(MAX_REQUESTS_PER_CONNECTION);
        optionalOptions.add(MAX_QUEUE_SIZE);
        optionalOptions.add(READ_TIME_OUT_MILLISECONDS);
        optionalOptions.add(CONNECT_TIMEOUT_MILLISECONDS);
        optionalOptions.add(POOL_TIMEOUT_MILLISECONDS);
        optionalOptions.add(USE_SSL);
        optionalOptions.add(ASYNC_WRITE);

        optionalOptions.add(SCAN_PARTITION_COLUMN);
        optionalOptions.add(SCAN_PARTITION_STRATEGY);
        optionalOptions.add(SCAN_INCREMENT_COLUMN);
        optionalOptions.add(SCAN_INCREMENT_COLUMN_TYPE);
        optionalOptions.add(SCAN_POLLING_INTERVAL);
        optionalOptions.add(SCAN_START_LOCATION);
        optionalOptions.add(SCAN_QUERY_TIMEOUT);
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

        return optionalOptions;
    }
}
