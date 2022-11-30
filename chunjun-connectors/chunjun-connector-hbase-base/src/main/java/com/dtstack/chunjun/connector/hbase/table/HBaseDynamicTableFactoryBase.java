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

package com.dtstack.chunjun.connector.hbase.table;

import com.dtstack.chunjun.connector.hbase.HBaseTableSchema;
import com.dtstack.chunjun.connector.hbase.config.HBaseConfig;
import com.dtstack.chunjun.lookup.config.LookupConfig;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.utils.TableSchemaUtils;

import org.apache.hadoop.hbase.HConstants;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.dtstack.chunjun.connector.hbase.table.HBaseOptions.NULL_STRING_LITERAL;
import static com.dtstack.chunjun.connector.hbase.table.HBaseOptions.SINK_BUFFER_FLUSH_INTERVAL;
import static com.dtstack.chunjun.connector.hbase.table.HBaseOptions.SINK_BUFFER_FLUSH_MAX_ROWS;
import static com.dtstack.chunjun.connector.hbase.table.HBaseOptions.SINK_BUFFER_FLUSH_MAX_SIZE;
import static com.dtstack.chunjun.connector.hbase.table.HBaseOptions.TABLE_NAME;
import static com.dtstack.chunjun.connector.hbase.table.HBaseOptions.ZOOKEEPER_QUORUM;
import static com.dtstack.chunjun.connector.hbase.table.HBaseOptions.ZOOKEEPER_ZNODE_PARENT;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_ASYNC_TIMEOUT;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_CACHE_MAX_ROWS;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_CACHE_PERIOD;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_CACHE_TTL;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_CACHE_TYPE;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_ERROR_LIMIT;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_FETCH_SIZE;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_MAX_RETRIES;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_PARALLELISM;
import static com.dtstack.chunjun.security.KerberosOptions.KEYTAB;
import static com.dtstack.chunjun.security.KerberosOptions.KRB5_CONF;
import static com.dtstack.chunjun.security.KerberosOptions.PRINCIPAL;
import static com.dtstack.chunjun.table.options.SinkOptions.SINK_PARALLELISM;

public abstract class HBaseDynamicTableFactoryBase
        implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    public static final String PROPERTIES_PREFIX = "properties.";

    @Override
    public abstract String factoryIdentifier();

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> set = new HashSet<>();
        set.add(TABLE_NAME);
        set.add(ZOOKEEPER_QUORUM);
        return set;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> set = new HashSet<>();
        set.add(ZOOKEEPER_ZNODE_PARENT);
        set.add(NULL_STRING_LITERAL);

        set.add(SINK_BUFFER_FLUSH_MAX_SIZE);
        set.add(SINK_BUFFER_FLUSH_INTERVAL);
        set.add(SINK_BUFFER_FLUSH_MAX_ROWS);
        set.add(SINK_PARALLELISM);

        set.add(LOOKUP_CACHE_PERIOD);
        set.add(LOOKUP_CACHE_MAX_ROWS);
        set.add(LOOKUP_CACHE_TTL);
        set.add(LOOKUP_CACHE_TYPE);
        set.add(LOOKUP_MAX_RETRIES);
        set.add(LOOKUP_ERROR_LIMIT);
        set.add(LOOKUP_FETCH_SIZE);
        set.add(LOOKUP_ASYNC_TIMEOUT);
        set.add(LOOKUP_PARALLELISM);

        set.add(PRINCIPAL);
        set.add(KEYTAB);
        set.add(KRB5_CONF);
        return set;
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        final ReadableConfig config = helper.getOptions();
        helper.validateExcept("properties.");
        TableSchema physicalSchema =
                TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());
        validatePrimaryKey(physicalSchema);
        Map<String, String> options = context.getCatalogTable().getOptions();
        HBaseConfig conf = getHbaseConf(config, options);
        LookupConfig lookupConfig =
                getLookupConf(config, context.getObjectIdentifier().getObjectName());
        HBaseTableSchema hbaseSchema = HBaseTableSchema.fromTableSchema(physicalSchema);
        String nullStringLiteral = helper.getOptions().get(NULL_STRING_LITERAL);
        return new HBaseDynamicTableSource(
                conf, physicalSchema, lookupConfig, hbaseSchema, nullStringLiteral);
    }

    private static void validatePrimaryKey(TableSchema schema) {
        HBaseTableSchema hbaseSchema = HBaseTableSchema.fromTableSchema(schema);
        if (!hbaseSchema.getRowKeyName().isPresent()) {
            throw new IllegalArgumentException(
                    "HBase table requires to define a row key field. A row key field is defined as an atomic type, column families and qualifiers are defined as ROW type.");
        } else {
            schema.getPrimaryKey()
                    .ifPresent(
                            (k) -> {
                                if (k.getColumns().size() > 1) {
                                    throw new IllegalArgumentException(
                                            "HBase table doesn't support a primary Key on multiple columns. The primary key of HBase table must be defined on row key field.");
                                } else if (!hbaseSchema
                                        .getRowKeyName()
                                        .get()
                                        .equals(k.getColumns().get(0))) {
                                    throw new IllegalArgumentException(
                                            "Primary key of HBase table must be defined on the row key field. A row key field is defined as an atomic type, column families and qualifiers are defined as ROW type.");
                                }
                            });
        }
    }

    private LookupConfig getLookupConf(ReadableConfig readableConfig, String tableName) {
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

    private HBaseConfig getHbaseConf(ReadableConfig config, Map<String, String> options) {
        HBaseConfig conf = new HBaseConfig();
        conf.setHbaseConfig(getHBaseClientProperties(options));
        String hTableName = config.get(TABLE_NAME);
        conf.setTable(hTableName);
        String nullStringLiteral = config.get(NULL_STRING_LITERAL);
        conf.setNullStringLiteral(nullStringLiteral);
        return conf;
    }

    private static Map<String, Object> getHBaseClientProperties(Map<String, String> tableOptions) {
        final Map<String, Object> hbaseProperties = new HashMap<>();

        org.apache.flink.configuration.Configuration options =
                org.apache.flink.configuration.Configuration.fromMap(tableOptions);
        hbaseProperties.put(HConstants.ZOOKEEPER_QUORUM, options.getString(ZOOKEEPER_QUORUM));
        hbaseProperties.put(
                HConstants.ZOOKEEPER_ZNODE_PARENT, options.getString(ZOOKEEPER_ZNODE_PARENT));
        // for hbase 2.x
        hbaseProperties.put(
                "hbase." + HConstants.ZOOKEEPER_ZNODE_PARENT,
                options.getString(ZOOKEEPER_ZNODE_PARENT));

        if (containsHBaseClientProperties(tableOptions)) {
            tableOptions.keySet().stream()
                    .filter(key -> key.startsWith(PROPERTIES_PREFIX))
                    .forEach(
                            key -> {
                                final String value = tableOptions.get(key);
                                final String subKey = key.substring((PROPERTIES_PREFIX).length());
                                hbaseProperties.put(subKey, value);
                            });
        }
        return hbaseProperties;
    }

    private static boolean containsHBaseClientProperties(Map<String, String> tableOptions) {
        return tableOptions.keySet().stream().anyMatch(k -> k.startsWith(PROPERTIES_PREFIX));
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        final ReadableConfig config = helper.getOptions();
        helper.validateExcept("properties.");
        TableSchema physicalSchema =
                TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());
        HBaseTableSchema hbaseSchema = HBaseTableSchema.fromTableSchema(physicalSchema);
        Map<String, String> options = context.getCatalogTable().getOptions();

        HBaseConfig conf = getHbaseConf(config, options);
        config.getOptional(SINK_PARALLELISM).ifPresent(conf::setParallelism);
        config.getOptional(SINK_BUFFER_FLUSH_MAX_ROWS).ifPresent(conf::setBatchSize);
        long millis = config.get(SINK_BUFFER_FLUSH_INTERVAL).toMillis();
        conf.setFlushIntervalMills(millis);
        long bufferFlushMaxSizeInBytes = config.get(SINK_BUFFER_FLUSH_MAX_SIZE).getBytes();
        conf.setWriteBufferSize(bufferFlushMaxSizeInBytes);

        conf.setRowkeyExpress(generateRowKey(hbaseSchema));
        String nullStringLiteral = helper.getOptions().get(NULL_STRING_LITERAL);
        return new HBaseDynamicTableSink(conf, physicalSchema, hbaseSchema, nullStringLiteral);
    }

    private String generateRowKey(HBaseTableSchema hbaseSchema) {
        int rowIndex = Math.max(hbaseSchema.getRowKeyIndex(), 1);
        String familyName = hbaseSchema.getFamilyNames()[rowIndex - 1];
        String[] qualifierNames = hbaseSchema.getQualifierNames(familyName);
        return Arrays.stream(qualifierNames)
                .map(key -> "${" + key + "}")
                .collect(Collectors.joining("_"));
    }
}
