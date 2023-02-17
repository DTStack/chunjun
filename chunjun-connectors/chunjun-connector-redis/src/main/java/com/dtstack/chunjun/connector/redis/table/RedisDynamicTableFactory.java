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

package com.dtstack.chunjun.connector.redis.table;

import com.dtstack.chunjun.connector.redis.config.RedisConfig;
import com.dtstack.chunjun.connector.redis.enums.RedisConnectType;
import com.dtstack.chunjun.connector.redis.enums.RedisDataMode;
import com.dtstack.chunjun.connector.redis.enums.RedisDataType;
import com.dtstack.chunjun.connector.redis.sink.RedisDynamicTableSink;
import com.dtstack.chunjun.connector.redis.source.RedisDynamicTableSource;
import com.dtstack.chunjun.lookup.config.LookupConfig;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.util.CollectionUtil;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.dtstack.chunjun.connector.redis.options.RedisOptions.DATABASE;
import static com.dtstack.chunjun.connector.redis.options.RedisOptions.KEYEXPIREDTIME;
import static com.dtstack.chunjun.connector.redis.options.RedisOptions.MASTERNAME;
import static com.dtstack.chunjun.connector.redis.options.RedisOptions.MAXIDLE;
import static com.dtstack.chunjun.connector.redis.options.RedisOptions.MAXTOTAL;
import static com.dtstack.chunjun.connector.redis.options.RedisOptions.MINIDLE;
import static com.dtstack.chunjun.connector.redis.options.RedisOptions.PASSWORD;
import static com.dtstack.chunjun.connector.redis.options.RedisOptions.REDISTYPE;
import static com.dtstack.chunjun.connector.redis.options.RedisOptions.REDIS_DATA_MODE;
import static com.dtstack.chunjun.connector.redis.options.RedisOptions.REDIS_DATA_TYPE;
import static com.dtstack.chunjun.connector.redis.options.RedisOptions.TABLENAME;
import static com.dtstack.chunjun.connector.redis.options.RedisOptions.TIMEOUT;
import static com.dtstack.chunjun.connector.redis.options.RedisOptions.URL;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_ASYNC_TIMEOUT;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_CACHE_MAX_ROWS;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_CACHE_PERIOD;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_CACHE_TTL;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_CACHE_TYPE;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_ERROR_LIMIT;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_FETCH_SIZE;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_MAX_RETRIES;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_PARALLELISM;
import static org.apache.flink.table.factories.FactoryUtil.SINK_PARALLELISM;

public class RedisDynamicTableFactory
        implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    private static final String IDENTIFIER = "redis-x";

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        final ReadableConfig config = helper.getOptions();

        helper.validate();

        ResolvedSchema resolvedSchema = context.getCatalogTable().getResolvedSchema();

        validateTableSchema(resolvedSchema);
        validateConfig(config);

        return new RedisDynamicTableSink(resolvedSchema, getRedisConfig(config, resolvedSchema));
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        final ReadableConfig config = helper.getOptions();

        helper.validate();
        validateConfig(config);

        ResolvedSchema resolvedSchema = context.getCatalogTable().getResolvedSchema();
        return new RedisDynamicTableSource(
                resolvedSchema,
                getRedisConfig(config, resolvedSchema),
                getRedisLookupConf(config, context.getObjectIdentifier().getObjectName()));
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> requiredOptions = new HashSet<>();
        requiredOptions.add(URL);
        requiredOptions.add(TABLENAME);
        requiredOptions.add(REDIS_DATA_TYPE);
        requiredOptions.add(REDIS_DATA_MODE);
        return requiredOptions;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> optionalOptions = new HashSet<>();
        optionalOptions.add(PASSWORD);
        optionalOptions.add(REDISTYPE);
        optionalOptions.add(MASTERNAME);
        optionalOptions.add(DATABASE);
        optionalOptions.add(SINK_PARALLELISM);
        optionalOptions.add(TIMEOUT);
        optionalOptions.add(MAXTOTAL);
        optionalOptions.add(MAXIDLE);
        optionalOptions.add(MINIDLE);
        optionalOptions.add(KEYEXPIREDTIME);

        optionalOptions.add(LOOKUP_CACHE_PERIOD);
        optionalOptions.add(LOOKUP_CACHE_MAX_ROWS);
        optionalOptions.add(LOOKUP_CACHE_TTL);
        optionalOptions.add(LOOKUP_CACHE_TYPE);
        optionalOptions.add(LOOKUP_MAX_RETRIES);
        optionalOptions.add(LOOKUP_ERROR_LIMIT);
        optionalOptions.add(LOOKUP_FETCH_SIZE);
        optionalOptions.add(LOOKUP_ASYNC_TIMEOUT);
        optionalOptions.add(LOOKUP_PARALLELISM);
        return optionalOptions;
    }

    private RedisConfig getRedisConfig(ReadableConfig config, ResolvedSchema schema) {
        RedisConfig redisConfig = new RedisConfig();
        redisConfig.setHostPort(config.get(URL));
        redisConfig.setTableName(config.get(TABLENAME));
        redisConfig.setPassword(config.get(PASSWORD));
        redisConfig.setRedisConnectType(RedisConnectType.parse(config.get(REDISTYPE)));
        redisConfig.setMasterName(config.get(MASTERNAME));
        redisConfig.setDatabase(config.get(DATABASE));
        redisConfig.setParallelism(config.get(SINK_PARALLELISM));
        redisConfig.setTimeout(config.get(TIMEOUT));
        redisConfig.setMaxTotal(config.get(MAXTOTAL));
        redisConfig.setMaxIdle(config.get(MAXIDLE));
        redisConfig.setMinIdle(config.get(MINIDLE));
        redisConfig.setExpireTime(config.get(KEYEXPIREDTIME));
        redisConfig.setType(RedisDataType.getDataType(config.get(REDIS_DATA_TYPE)));
        redisConfig.setMode(RedisDataMode.getDataMode(config.get(REDIS_DATA_MODE)));
        redisConfig.setKeyPrefix(config.get(TABLENAME));
        List<String> keyFields =
                schema.getPrimaryKey()
                        .map(UniqueConstraint::getColumns)
                        .orElse(Collections.emptyList());
        redisConfig.setUpdateKey(keyFields);
        return redisConfig;
    }

    private LookupConfig getRedisLookupConf(ReadableConfig readableConfig, String tableName) {
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

    private void validateTableSchema(ResolvedSchema physicalSchema) {
        List<String> keyFields =
                physicalSchema
                        .getPrimaryKey()
                        .map(UniqueConstraint::getColumns)
                        .orElse(Collections.emptyList());
        Preconditions.checkState(
                !CollectionUtil.isNullOrEmpty(keyFields),
                "please declare primary key for redis sink table .");
    }

    private void validateConfig(ReadableConfig config) {
        if (config.get(REDISTYPE) == 2) {
            Preconditions.checkArgument(
                    !StringUtils.isEmpty(config.get(MASTERNAME)),
                    "redis field of masterName is required when redisType is 2.");
        }
    }
}
