/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.dtstack.chunjun.connector.vertica11.table;

import com.dtstack.chunjun.connector.jdbc.dialect.JdbcDialect;
import com.dtstack.chunjun.connector.jdbc.sink.JdbcOutputFormatBuilder;
import com.dtstack.chunjun.connector.jdbc.table.JdbcDynamicTableFactory;
import com.dtstack.chunjun.connector.vertica11.config.Vertica11LookupConfig;
import com.dtstack.chunjun.connector.vertica11.dialect.Vertica11Dialect;
import com.dtstack.chunjun.connector.vertica11.sink.Vertica11OutputFormat;
import com.dtstack.chunjun.connector.vertica11.source.Vertica11DynamicTableSource;
import com.dtstack.chunjun.lookup.config.LookupConfig;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.FactoryUtil;

import static com.dtstack.chunjun.connector.vertica11.lookup.options.Vertica11LookupOptions.VERTX_WORKER_POOL_SIZE;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_ASYNC_TIMEOUT;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_CACHE_MAX_ROWS;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_CACHE_PERIOD;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_CACHE_TTL;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_CACHE_TYPE;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_ERROR_LIMIT;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_FETCH_SIZE;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_MAX_RETRIES;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_PARALLELISM;

public class Vertica11DynamicTableFactory extends JdbcDynamicTableFactory {

    /** Find a specific plugin by this value */
    private static final String IDENTIFIER = "vertica11-x";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    protected JdbcDialect getDialect() {
        return new Vertica11Dialect();
    }

    protected JdbcOutputFormatBuilder getOutputFormatBuilder() {
        return new JdbcOutputFormatBuilder(new Vertica11OutputFormat());
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        final ReadableConfig config = helper.getOptions();

        ResolvedSchema resolvedSchema = context.getCatalogTable().getResolvedSchema();

        validateConfigOptions(config, resolvedSchema);

        JdbcDialect jdbcDialect = getDialect();

        return new Vertica11DynamicTableSource(
                getSourceConnectionConfig(helper.getOptions()),
                getJdbcLookupConf(
                        helper.getOptions(), context.getObjectIdentifier().getObjectName()),
                resolvedSchema,
                jdbcDialect,
                getInputFormatBuilder());
    }

    protected LookupConfig getJdbcLookupConf(ReadableConfig readableConfig, String tableName) {
        return Vertica11LookupConfig.build()
                .setAsyncPoolSize(readableConfig.get(VERTX_WORKER_POOL_SIZE))
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
