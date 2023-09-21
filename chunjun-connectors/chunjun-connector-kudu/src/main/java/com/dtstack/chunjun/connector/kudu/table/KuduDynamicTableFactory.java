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

package com.dtstack.chunjun.connector.kudu.table;

import com.dtstack.chunjun.connector.kudu.config.KuduLookupConfig;
import com.dtstack.chunjun.connector.kudu.config.KuduSinkConfig;
import com.dtstack.chunjun.connector.kudu.config.KuduSourceConfig;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.HashSet;
import java.util.Set;

import static com.dtstack.chunjun.connector.kudu.table.KuduOptions.ADMIN_OPERATION_TIMEOUT;
import static com.dtstack.chunjun.connector.kudu.table.KuduOptions.FILTER_EXPRESSION;
import static com.dtstack.chunjun.connector.kudu.table.KuduOptions.FLUSH_MODE;
import static com.dtstack.chunjun.connector.kudu.table.KuduOptions.MASTER_ADDRESS;
import static com.dtstack.chunjun.connector.kudu.table.KuduOptions.MUTATION_BUFFER_SPACE;
import static com.dtstack.chunjun.connector.kudu.table.KuduOptions.OPERATION_TIMEOUT;
import static com.dtstack.chunjun.connector.kudu.table.KuduOptions.QUERY_TIMEOUT;
import static com.dtstack.chunjun.connector.kudu.table.KuduOptions.READ_MODE;
import static com.dtstack.chunjun.connector.kudu.table.KuduOptions.SCANNER_BATCH_SIZE_BYTES;
import static com.dtstack.chunjun.connector.kudu.table.KuduOptions.TABLE_NAME;
import static com.dtstack.chunjun.connector.kudu.table.KuduOptions.WORKER_COUNT;
import static com.dtstack.chunjun.connector.kudu.table.KuduOptions.WRITE_MODE;
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
import static com.dtstack.chunjun.source.options.SourceOptions.SCAN_FETCH_SIZE;
import static com.dtstack.chunjun.source.options.SourceOptions.SCAN_PARALLELISM;
import static com.dtstack.chunjun.source.options.SourceOptions.SCAN_QUERY_TIMEOUT;
import static com.dtstack.chunjun.source.options.SourceOptions.SCAN_START_LOCATION;
import static com.dtstack.chunjun.table.options.SinkOptions.SINK_BUFFER_FLUSH_INTERVAL;
import static com.dtstack.chunjun.table.options.SinkOptions.SINK_BUFFER_FLUSH_MAX_ROWS;
import static com.dtstack.chunjun.table.options.SinkOptions.SINK_MAX_RETRIES;
import static com.dtstack.chunjun.table.options.SinkOptions.SINK_PARALLELISM;

public class KuduDynamicTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    private static final String IDENTIFIER = "kudu-x";

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);

        final ReadableConfig options = helper.getOptions();

        helper.validate();

        return new KuduDynamicTableSink(
                KuduSinkConfig.from(options), context.getCatalogTable().getResolvedSchema());
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);

        final ReadableConfig options = helper.getOptions();

        helper.validate();

        KuduSourceConfig kuduSourceConfig = KuduSourceConfig.from(options);
        KuduLookupConfig kuduLookupConfig = KuduLookupConfig.from(options);

        return new KuduDynamicTableSource(
                kuduSourceConfig, kuduLookupConfig, context.getCatalogTable().getResolvedSchema());
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
        optionalOptions.add(FILTER_EXPRESSION);
        optionalOptions.add(SCANNER_BATCH_SIZE_BYTES);
        optionalOptions.add(FLUSH_MODE);
        optionalOptions.add(SCAN_PARALLELISM);
        optionalOptions.add(MUTATION_BUFFER_SPACE);

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
        optionalOptions.add(SINK_PARALLELISM);
        optionalOptions.add(WRITE_MODE);

        // kerberos
        optionalOptions.add(PRINCIPAL);
        optionalOptions.add(KRB5_CONF);
        optionalOptions.add(KEYTAB);

        return optionalOptions;
    }
}
