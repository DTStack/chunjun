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
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.utils.TableSchemaUtils;

import org.apache.hadoop.hbase.HConstants;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.dtstack.chunjun.connector.hbase.config.HBaseConfigConstants.MULTI_VERSION_FIXED_COLUMN;
import static com.dtstack.chunjun.connector.hbase.table.HBaseOptions.END_ROW_KEY;
import static com.dtstack.chunjun.connector.hbase.table.HBaseOptions.IS_BINARY_ROW_KEY;
import static com.dtstack.chunjun.connector.hbase.table.HBaseOptions.MAX_VERSION;
import static com.dtstack.chunjun.connector.hbase.table.HBaseOptions.MODE;
import static com.dtstack.chunjun.connector.hbase.table.HBaseOptions.NULL_MODE;
import static com.dtstack.chunjun.connector.hbase.table.HBaseOptions.NULL_STRING_LITERAL;
import static com.dtstack.chunjun.connector.hbase.table.HBaseOptions.ROWKEY_EXPRESS;
import static com.dtstack.chunjun.connector.hbase.table.HBaseOptions.SCAN_BATCH_SIZE;
import static com.dtstack.chunjun.connector.hbase.table.HBaseOptions.SCAN_CACHE_SIZE;
import static com.dtstack.chunjun.connector.hbase.table.HBaseOptions.SINK_BUFFER_FLUSH_INTERVAL;
import static com.dtstack.chunjun.connector.hbase.table.HBaseOptions.SINK_BUFFER_FLUSH_MAX_ROWS;
import static com.dtstack.chunjun.connector.hbase.table.HBaseOptions.SINK_BUFFER_FLUSH_MAX_SIZE;
import static com.dtstack.chunjun.connector.hbase.table.HBaseOptions.START_ROW_KEY;
import static com.dtstack.chunjun.connector.hbase.table.HBaseOptions.TABLE_NAME;
import static com.dtstack.chunjun.connector.hbase.table.HBaseOptions.VERSION_COLUMN_NAME;
import static com.dtstack.chunjun.connector.hbase.table.HBaseOptions.VERSION_COLUMN_VALUE;
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

        set.add(START_ROW_KEY);
        set.add(END_ROW_KEY);
        set.add(IS_BINARY_ROW_KEY);
        set.add(SCAN_CACHE_SIZE);
        set.add(VERSION_COLUMN_NAME);
        set.add(VERSION_COLUMN_VALUE);
        set.add(ROWKEY_EXPRESS);
        set.add(SCAN_BATCH_SIZE);
        set.add(MODE);
        set.add(MAX_VERSION);
        set.add(NULL_MODE);
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
        Map<String, String> options = context.getCatalogTable().getOptions();
        HBaseConfig conf = getHbaseConf(config, options);
        LookupConfig lookupConfig =
                getLookupConf(config, context.getObjectIdentifier().getObjectName());
        HBaseTableSchema hbaseSchema =
                validatePrimaryKey(
                        context.getPhysicalRowDataType(), context.getPrimaryKeyIndexes(), conf);
        return new HBaseDynamicTableSource(conf, physicalSchema, lookupConfig, hbaseSchema);
    }

    private static HBaseTableSchema validatePrimaryKey(TableSchema schema, HBaseConfig conf) {
        HBaseTableSchema hbaseSchema = HBaseTableSchema.fromTableSchema(schema, conf);
        if (conf.getMode().equalsIgnoreCase(MULTI_VERSION_FIXED_COLUMN)) {
            return hbaseSchema;
        }
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
        return hbaseSchema;
    }

    /**
     * Checks that the HBase table have row key defined. A row key is defined as an atomic type, and
     * column families and qualifiers are defined as ROW type. There shouldn't be multiple atomic
     * type columns in the schema. The PRIMARY KEY constraint is optional, if exist, the primary key
     * constraint must be defined on the single row key field.
     */
    public static HBaseTableSchema validatePrimaryKey(
            DataType dataType, int[] primaryKeyIndexes, HBaseConfig conf) {
        HBaseTableSchema hbaseSchema = HBaseTableSchema.fromDataType(dataType, conf);
        if (conf.getMode().equalsIgnoreCase(MULTI_VERSION_FIXED_COLUMN)) {
            return hbaseSchema;
        }
        if (!hbaseSchema.getRowKeyName().isPresent()) {
            throw new IllegalArgumentException(
                    "HBase table requires to define a row key field. "
                            + "A row key field is defined as an atomic type, "
                            + "column families and qualifiers are defined as ROW type.");
        }
        if (primaryKeyIndexes.length == 0) {
            return hbaseSchema;
        }
        if (primaryKeyIndexes.length > 1) {
            throw new IllegalArgumentException(
                    "HBase table doesn't support a primary Key on multiple columns. "
                            + "The primary key of HBase table must be defined on row key field.");
        }
        if (!hbaseSchema
                .getRowKeyName()
                .get()
                .equals(DataType.getFieldNames(dataType).get(primaryKeyIndexes[0]))) {
            throw new IllegalArgumentException(
                    "Primary key of HBase table must be defined on the row key field. "
                            + "A row key field is defined as an atomic type, "
                            + "column families and qualifiers are defined as ROW type.");
        }
        return hbaseSchema;
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
        conf.setStartRowkey(config.get(START_ROW_KEY));
        conf.setEndRowkey(config.get(END_ROW_KEY));
        conf.setBinaryRowkey(config.get(IS_BINARY_ROW_KEY));
        conf.setScanCacheSize(config.get(SCAN_CACHE_SIZE));
        conf.setVersionColumnName(config.get(VERSION_COLUMN_NAME));
        conf.setVersionColumnValue(config.get(VERSION_COLUMN_VALUE));
        conf.setRowkeyExpress(config.get(ROWKEY_EXPRESS));
        conf.setScanBatchSize(config.get(SCAN_BATCH_SIZE));
        conf.setMode(config.get(MODE));
        conf.setMaxVersion(config.get(MAX_VERSION));
        conf.setNullMode(config.get(NULL_MODE));
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
        Map<String, String> options = context.getCatalogTable().getOptions();

        HBaseConfig conf = getHbaseConf(config, options);
        config.getOptional(SINK_PARALLELISM).ifPresent(conf::setParallelism);
        config.getOptional(SINK_BUFFER_FLUSH_MAX_ROWS).ifPresent(conf::setBatchSize);
        long millis = config.get(SINK_BUFFER_FLUSH_INTERVAL).toMillis();
        conf.setFlushIntervalMills(millis);
        long bufferFlushMaxSizeInBytes = config.get(SINK_BUFFER_FLUSH_MAX_SIZE).getBytes();
        conf.setWriteBufferSize(bufferFlushMaxSizeInBytes);
        String nullStringLiteral = helper.getOptions().get(NULL_STRING_LITERAL);
        conf.setNullStringLiteral(nullStringLiteral);
        HBaseTableSchema hbaseSchema = HBaseTableSchema.fromTableSchema(physicalSchema, conf);
        return new HBaseDynamicTableSink(conf, physicalSchema, hbaseSchema);
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
