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

package com.dtstack.flinkx.connector.redis.table;

import com.dtstack.flinkx.connector.redis.conf.RedisConf;
import com.dtstack.flinkx.connector.redis.enums.RedisConnectType;
import com.dtstack.flinkx.connector.redis.sink.RedisDynamicTableSink;
import com.dtstack.flinkx.connector.redis.source.RedisDynamicTableSource;
import com.dtstack.flinkx.lookup.conf.LookupConf;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.util.CollectionUtil;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.dtstack.flinkx.connector.redis.options.RedisOptions.DATABASE;
import static com.dtstack.flinkx.connector.redis.options.RedisOptions.KEYEXPIREDTIME;
import static com.dtstack.flinkx.connector.redis.options.RedisOptions.MASTERNAME;
import static com.dtstack.flinkx.connector.redis.options.RedisOptions.MAXIDLE;
import static com.dtstack.flinkx.connector.redis.options.RedisOptions.MAXTOTAL;
import static com.dtstack.flinkx.connector.redis.options.RedisOptions.MINIDLE;
import static com.dtstack.flinkx.connector.redis.options.RedisOptions.PASSWORD;
import static com.dtstack.flinkx.connector.redis.options.RedisOptions.REDISTYPE;
import static com.dtstack.flinkx.connector.redis.options.RedisOptions.TABLENAME;
import static com.dtstack.flinkx.connector.redis.options.RedisOptions.TIMEOUT;
import static com.dtstack.flinkx.connector.redis.options.RedisOptions.URL;
import static com.dtstack.flinkx.lookup.options.LookupOptions.LOOKUP_ASYNC_TIMEOUT;
import static com.dtstack.flinkx.lookup.options.LookupOptions.LOOKUP_CACHE_MAX_ROWS;
import static com.dtstack.flinkx.lookup.options.LookupOptions.LOOKUP_CACHE_PERIOD;
import static com.dtstack.flinkx.lookup.options.LookupOptions.LOOKUP_CACHE_TTL;
import static com.dtstack.flinkx.lookup.options.LookupOptions.LOOKUP_CACHE_TYPE;
import static com.dtstack.flinkx.lookup.options.LookupOptions.LOOKUP_ERROR_LIMIT;
import static com.dtstack.flinkx.lookup.options.LookupOptions.LOOKUP_FETCH_SIZE;
import static com.dtstack.flinkx.lookup.options.LookupOptions.LOOKUP_MAX_RETRIES;
import static com.dtstack.flinkx.lookup.options.LookupOptions.LOOKUP_PARALLELISM;
import static org.apache.flink.table.factories.FactoryUtil.SINK_PARALLELISM;

/**
 * @author chuixue
 * @create 2021-06-16 15:07
 * @description
 */
public class RedisDynamicTableFactory
        implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    private static final String IDENTIFIER = "redis-x";

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        final ReadableConfig config = helper.getOptions();

        helper.validate();

        TableSchema physicalSchema =
                TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());

        validateTableSchema(physicalSchema);
        validateConfig(config);

        return new RedisDynamicTableSink(physicalSchema, getRedisConf(config, physicalSchema));
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        final ReadableConfig config = helper.getOptions();

        helper.validate();
        validateConfig(config);

        TableSchema physicalSchema =
                TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());

        return new RedisDynamicTableSource(
                physicalSchema,
                getRedisConf(config, physicalSchema),
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

    private RedisConf getRedisConf(ReadableConfig config, TableSchema schema) {
        RedisConf redisConf = new RedisConf();
        redisConf.setHostPort(config.get(URL));
        redisConf.setTableName(config.get(TABLENAME));
        redisConf.setPassword(config.get(PASSWORD));
        redisConf.setRedisConnectType(RedisConnectType.parse(config.get(REDISTYPE)));
        redisConf.setMasterName(config.get(MASTERNAME));
        redisConf.setDatabase(config.get(DATABASE));
        redisConf.setParallelism(config.get(SINK_PARALLELISM));
        redisConf.setTimeout(config.get(TIMEOUT));
        redisConf.setMaxTotal(config.get(MAXTOTAL));
        redisConf.setMaxIdle(config.get(MAXIDLE));
        redisConf.setMinIdle(config.get(MINIDLE));
        redisConf.setExpireTime(config.get(KEYEXPIREDTIME));

        List<String> keyFields = schema.getPrimaryKey().map(pk -> pk.getColumns()).orElse(null);
        redisConf.setUpdateKey(keyFields);
        return redisConf;
    }

    private LookupConf getRedisLookupConf(ReadableConfig readableConfig, String tableName) {
        return LookupConf.build()
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

    private void validateTableSchema(TableSchema physicalSchema) {
        List<String> keyFields =
                physicalSchema.getPrimaryKey().map(pk -> pk.getColumns()).orElse(null);
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
