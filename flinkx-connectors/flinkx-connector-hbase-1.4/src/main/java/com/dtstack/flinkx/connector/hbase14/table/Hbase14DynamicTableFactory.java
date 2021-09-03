/*
 *    Copyright 2021 the original author or authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package com.dtstack.flinkx.connector.hbase14.table;

import com.dtstack.flinkx.connector.hbase14.HBaseTableSchema;
import com.dtstack.flinkx.connector.hbase14.conf.HBaseConf;
import com.dtstack.flinkx.connector.hbase14.sink.HBaseDynamicTableSink;
import com.dtstack.flinkx.connector.hbase14.source.HBaseDynamicTableSource;
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

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.dtstack.flinkx.connector.hbase14.table.HBaseOptions.NULL_STRING_LITERAL;
import static com.dtstack.flinkx.connector.hbase14.table.HBaseOptions.SINK_BUFFER_FLUSH_INTERVAL;
import static com.dtstack.flinkx.connector.hbase14.table.HBaseOptions.SINK_BUFFER_FLUSH_MAX_ROWS;
import static com.dtstack.flinkx.connector.hbase14.table.HBaseOptions.SINK_BUFFER_FLUSH_MAX_SIZE;
import static com.dtstack.flinkx.connector.hbase14.table.HBaseOptions.TABLE_NAME;
import static com.dtstack.flinkx.connector.hbase14.table.HBaseOptions.ZOOKEEPER_QUORUM;
import static com.dtstack.flinkx.connector.hbase14.table.HBaseOptions.ZOOKEEPER_ZNODE_PARENT;
import static com.dtstack.flinkx.lookup.options.LookupOptions.LOOKUP_ASYNC_TIMEOUT;
import static com.dtstack.flinkx.lookup.options.LookupOptions.LOOKUP_CACHE_MAX_ROWS;
import static com.dtstack.flinkx.lookup.options.LookupOptions.LOOKUP_CACHE_PERIOD;
import static com.dtstack.flinkx.lookup.options.LookupOptions.LOOKUP_CACHE_TTL;
import static com.dtstack.flinkx.lookup.options.LookupOptions.LOOKUP_CACHE_TYPE;
import static com.dtstack.flinkx.lookup.options.LookupOptions.LOOKUP_ERROR_LIMIT;
import static com.dtstack.flinkx.lookup.options.LookupOptions.LOOKUP_FETCH_SIZE;
import static com.dtstack.flinkx.lookup.options.LookupOptions.LOOKUP_MAX_RETRIES;
import static com.dtstack.flinkx.lookup.options.LookupOptions.LOOKUP_PARALLELISM;

public class Hbase14DynamicTableFactory
        implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    public static final String IDENTIFIER = "hbase14-x";
    public static final String PROPERTIES_PREFIX = "properties.";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> set = new HashSet<>();
        set.add(TABLE_NAME);
        return set;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> set = new HashSet<>();
        set.add(ZOOKEEPER_ZNODE_PARENT);
        set.add(ZOOKEEPER_QUORUM);
        set.add(NULL_STRING_LITERAL);
        set.add(SINK_BUFFER_FLUSH_MAX_SIZE);
        set.add(SINK_BUFFER_FLUSH_INTERVAL);
        set.add(SINK_BUFFER_FLUSH_MAX_ROWS);
        set.add(FactoryUtil.SINK_PARALLELISM);
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
        HBaseConf conf = getHbaseConf(config, options);
        LookupConf lookupConf =
                getLookupConf(config, context.getObjectIdentifier().getObjectName());
        return new HBaseDynamicTableSource(conf, physicalSchema, lookupConf);
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

    private LookupConf getLookupConf(ReadableConfig readableConfig, String tableName) {
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

    private HBaseConf getHbaseConf(ReadableConfig config, Map<String, String> options) {
        HBaseConf conf = new HBaseConf();
        conf.setHbaseConfig(getHBaseClientProperties(options));
        String hTableName = config.get(TABLE_NAME);
        String nullStringLiteral = config.get(NULL_STRING_LITERAL);
        conf.setTableName(hTableName);
        conf.setNullMode(nullStringLiteral);
        return conf;
    }

    private static Map<String, Object> getHBaseClientProperties(Map<String, String> tableOptions) {
        final Map<String, Object> hbaseProperties = new HashMap<>();
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
        HBaseConf conf = getHbaseConf(config, options);
        conf.setRowkeyExpress(generateRowKey(hbaseSchema));
        return new HBaseDynamicTableSink(conf, physicalSchema, hbaseSchema);
    }

    private String generateRowKey(HBaseTableSchema hbaseSchema) {
        int rowIndex = 1;
        if (hbaseSchema.getRowKeyIndex() > 1) {
            rowIndex = hbaseSchema.getRowKeyIndex();
        }
        String familyName = hbaseSchema.getFamilyNames()[rowIndex - 1];
        String[] qualifierNames = hbaseSchema.getQualifierNames(familyName);
        return Arrays.stream(qualifierNames)
                .map(key -> "${" + key + "}")
                .collect(Collectors.joining("_"));
    }
}
