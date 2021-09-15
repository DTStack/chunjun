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

package com.dtstack.flinkx.connector.kudu.table;

import com.dtstack.flinkx.connector.kudu.conf.KuduLookupConf;
import com.dtstack.flinkx.connector.kudu.conf.KuduSinkConf;
import com.dtstack.flinkx.connector.kudu.conf.KuduSourceConf;
import com.dtstack.flinkx.connector.kudu.sink.KuduDynamicTableSink;
import com.dtstack.flinkx.connector.kudu.source.KuduDynamicTableSource;

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

import static com.dtstack.flinkx.connector.kudu.options.KuduOptions.ADMIN_OPERATION_TIMEOUT;
import static com.dtstack.flinkx.connector.kudu.options.KuduOptions.BATCH_SIZE_BYTES;
import static com.dtstack.flinkx.connector.kudu.options.KuduOptions.FILTER_STRING;
import static com.dtstack.flinkx.connector.kudu.options.KuduOptions.FLUSH_INTERVAL;
import static com.dtstack.flinkx.connector.kudu.options.KuduOptions.FLUSH_MODE;
import static com.dtstack.flinkx.connector.kudu.options.KuduOptions.IGNORE_DUPLICATE;
import static com.dtstack.flinkx.connector.kudu.options.KuduOptions.IGNORE_NOT_FOUND;
import static com.dtstack.flinkx.connector.kudu.options.KuduOptions.MASTER_ADDRESS;
import static com.dtstack.flinkx.connector.kudu.options.KuduOptions.MAX_BUFFER_SIZE;
import static com.dtstack.flinkx.connector.kudu.options.KuduOptions.OPERATION_TIMEOUT;
import static com.dtstack.flinkx.connector.kudu.options.KuduOptions.QUERY_TIMEOUT;
import static com.dtstack.flinkx.connector.kudu.options.KuduOptions.READ_MODE;
import static com.dtstack.flinkx.connector.kudu.options.KuduOptions.SCAN_PARALLELISM;
import static com.dtstack.flinkx.connector.kudu.options.KuduOptions.TABLE_NAME;
import static com.dtstack.flinkx.connector.kudu.options.KuduOptions.WORKER_COUNT;
import static com.dtstack.flinkx.lookup.options.LookupOptions.LOOKUP_ASYNC_TIMEOUT;
import static com.dtstack.flinkx.lookup.options.LookupOptions.LOOKUP_CACHE_MAX_ROWS;
import static com.dtstack.flinkx.lookup.options.LookupOptions.LOOKUP_CACHE_PERIOD;
import static com.dtstack.flinkx.lookup.options.LookupOptions.LOOKUP_CACHE_TTL;
import static com.dtstack.flinkx.lookup.options.LookupOptions.LOOKUP_CACHE_TYPE;
import static com.dtstack.flinkx.lookup.options.LookupOptions.LOOKUP_ERROR_LIMIT;
import static com.dtstack.flinkx.lookup.options.LookupOptions.LOOKUP_FETCH_SIZE;
import static com.dtstack.flinkx.lookup.options.LookupOptions.LOOKUP_MAX_RETRIES;
import static com.dtstack.flinkx.lookup.options.LookupOptions.LOOKUP_PARALLELISM;
import static com.dtstack.flinkx.security.KerberosOptions.KEYTAB;
import static com.dtstack.flinkx.security.KerberosOptions.KRB5_CONF;
import static com.dtstack.flinkx.security.KerberosOptions.PRINCIPAL;
import static com.dtstack.flinkx.source.options.SourceOptions.SCAN_FETCH_SIZE;
import static com.dtstack.flinkx.source.options.SourceOptions.SCAN_QUERY_TIMEOUT;
import static com.dtstack.flinkx.source.options.SourceOptions.SCAN_START_LOCATION;
import static com.dtstack.flinkx.table.options.SinkOptions.SINK_BUFFER_FLUSH_INTERVAL;
import static com.dtstack.flinkx.table.options.SinkOptions.SINK_BUFFER_FLUSH_MAX_ROWS;
import static com.dtstack.flinkx.table.options.SinkOptions.SINK_MAX_RETRIES;

/**
 * @author tiezhu
 * @since 2021/6/9 星期三
 */
public class KuduDynamicTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    private static final String IDENTIFIER = "kudu-x";

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);

        final ReadableConfig options = helper.getOptions();

        helper.validate();

        TableSchema tableSchema =
                TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());

        KuduSinkConf kuduSinkConf = KuduSinkConf.from(options);

        return new KuduDynamicTableSink(kuduSinkConf, tableSchema);
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);

        final ReadableConfig options = helper.getOptions();

        helper.validate();

        TableSchema tableSchema =
                TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());

        KuduSourceConf kuduSourceConf = KuduSourceConf.from(options);
        KuduLookupConf kuduLookupConf = KuduLookupConf.from(options);

        return new KuduDynamicTableSource(kuduSourceConf, kuduLookupConf, tableSchema);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> requiredOptions = new HashSet<>();

        requiredOptions.add(MASTER_ADDRESS);
        requiredOptions.add(TABLE_NAME);

        return requiredOptions;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> optionalOptions = new HashSet<>();

        optionalOptions.add(WORKER_COUNT);
        optionalOptions.add(OPERATION_TIMEOUT);
        optionalOptions.add(ADMIN_OPERATION_TIMEOUT);
        optionalOptions.add(QUERY_TIMEOUT);
        optionalOptions.add(READ_MODE);
        optionalOptions.add(FILTER_STRING);
        optionalOptions.add(BATCH_SIZE_BYTES);
        optionalOptions.add(FLUSH_MODE);
        optionalOptions.add(SCAN_PARALLELISM);
        optionalOptions.add(MAX_BUFFER_SIZE);
        optionalOptions.add(FLUSH_INTERVAL);
        optionalOptions.add(IGNORE_NOT_FOUND);
        optionalOptions.add(IGNORE_DUPLICATE);

        optionalOptions.add(SCAN_START_LOCATION);
        optionalOptions.add(SCAN_QUERY_TIMEOUT);
        optionalOptions.add(SCAN_FETCH_SIZE);

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

        // kerberos
        optionalOptions.add(PRINCIPAL);
        optionalOptions.add(KRB5_CONF);
        optionalOptions.add(KEYTAB);

        return optionalOptions;
    }
}
