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

package com.dtstack.flinkx.connector.mongodb.table;

import com.dtstack.flinkx.connector.mongodb.conf.MongoClientConf;
import com.dtstack.flinkx.connector.mongodb.conf.MongoWriteConf;
import com.dtstack.flinkx.connector.mongodb.table.options.MongoClientOptions;
import com.dtstack.flinkx.lookup.conf.LookupConf;
import com.dtstack.flinkx.lookup.conf.LookupConfFactory;

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
import java.util.Set;

import static com.dtstack.flinkx.lookup.options.LookupOptions.LOOKUP_ASYNC_TIMEOUT;
import static com.dtstack.flinkx.lookup.options.LookupOptions.LOOKUP_CACHE_MAX_ROWS;
import static com.dtstack.flinkx.lookup.options.LookupOptions.LOOKUP_CACHE_PERIOD;
import static com.dtstack.flinkx.lookup.options.LookupOptions.LOOKUP_CACHE_TTL;
import static com.dtstack.flinkx.lookup.options.LookupOptions.LOOKUP_CACHE_TYPE;
import static com.dtstack.flinkx.lookup.options.LookupOptions.LOOKUP_ERROR_LIMIT;
import static com.dtstack.flinkx.lookup.options.LookupOptions.LOOKUP_FETCH_SIZE;
import static com.dtstack.flinkx.lookup.options.LookupOptions.LOOKUP_MAX_RETRIES;
import static com.dtstack.flinkx.lookup.options.LookupOptions.LOOKUP_PARALLELISM;
import static com.dtstack.flinkx.source.options.SourceOptions.SCAN_PARALLELISM;
import static com.dtstack.flinkx.table.options.SinkOptions.SINK_BUFFER_FLUSH_INTERVAL;
import static com.dtstack.flinkx.table.options.SinkOptions.SINK_BUFFER_FLUSH_MAX_ROWS;
import static com.dtstack.flinkx.table.options.SinkOptions.SINK_PARALLELISM;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * @author Ada Wong
 * @program flinkx
 * @create 2021/06/21
 */
public class MongodbDynamicTableFactory
        implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    /**
     * 解析、校验、读取参数
     *
     * @param context
     * @return
     */
    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        final ReadableConfig config = helper.getOptions();
        // 校验 requiredOptions 和 optionalOptions;
        helper.validate();
        TableSchema physicalSchema =
                TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());

        validateConfigOptions(config);

        MongoClientConf mongoClientConf = new MongoClientConf();
        config.getOptional(MongoClientOptions.URL).ifPresent(mongoClientConf::setUri);
        config.getOptional(MongoClientOptions.DATABASE).ifPresent(mongoClientConf::setDatabase);
        config.getOptional(MongoClientOptions.COLLECTION).ifPresent(mongoClientConf::setCollection);

        config.getOptional(MongoClientOptions.USERNAME).ifPresent(mongoClientConf::setUsername);
        config.getOptional(MongoClientOptions.PASSWORD).ifPresent(mongoClientConf::setPassword);

        LookupConf lookupConf = LookupConfFactory.createLookupConf(config);
        return new MongodbDynamicTableSource(mongoClientConf, lookupConf, physicalSchema);
    }

    /**
     * SPI加载识别
     *
     * @return
     */
    @Override
    public String factoryIdentifier() {
        return "mongodb-x";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> requiredOptions = new HashSet<>();
        requiredOptions.add(MongoClientOptions.COLLECTION);
        requiredOptions.add(MongoClientOptions.DATABASE);
        return requiredOptions;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> optionalOptions = new HashSet<>();
        optionalOptions.add(MongoClientOptions.URL);
        optionalOptions.add(MongoClientOptions.USERNAME);
        optionalOptions.add(MongoClientOptions.PASSWORD);

        optionalOptions.add(SCAN_PARALLELISM);

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
        optionalOptions.add(SINK_PARALLELISM);

        return optionalOptions;
    }

    /**
     * 针对特定Connector 进行的参数校验
     *
     * @param config
     */
    protected void validateConfigOptions(ReadableConfig config) {
        String url = config.get(MongoClientOptions.URL);
        if (url != null) {
            checkState(
                    url.startsWith("mongodb://"),
                    "Cannot handle such mongodb url must start with mongodb://");
        }
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        final ReadableConfig config = helper.getOptions();
        // 校验 requiredOptions 和 optionalOptions;
        helper.validate();
        TableSchema physicalSchema =
                TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());

        validateConfigOptions(config);

        MongoClientConf mongoClientConf = new MongoClientConf();
        config.getOptional(MongoClientOptions.URL).ifPresent(mongoClientConf::setUri);
        config.getOptional(MongoClientOptions.DATABASE).ifPresent(mongoClientConf::setDatabase);
        config.getOptional(MongoClientOptions.COLLECTION).ifPresent(mongoClientConf::setCollection);

        config.getOptional(MongoClientOptions.USERNAME).ifPresent(mongoClientConf::setUsername);
        config.getOptional(MongoClientOptions.PASSWORD).ifPresent(mongoClientConf::setPassword);
        MongoWriteConf mongoWriteConf = new MongoWriteConf();
        config.getOptional(SINK_PARALLELISM).ifPresent(mongoWriteConf::setParallelism);
        config.getOptional(SINK_BUFFER_FLUSH_MAX_ROWS).ifPresent(mongoWriteConf::setFlushMaxRows);
        config.getOptional(SINK_BUFFER_FLUSH_INTERVAL).ifPresent(mongoWriteConf::setFlushInterval);
        return new MongodbDynamicTableSink(mongoClientConf, physicalSchema, mongoWriteConf);
    }
}
