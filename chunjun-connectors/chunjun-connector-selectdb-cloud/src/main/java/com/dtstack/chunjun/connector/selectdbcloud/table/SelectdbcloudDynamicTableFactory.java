// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.dtstack.chunjun.connector.selectdbcloud.table;

import com.dtstack.chunjun.connector.selectdbcloud.options.SelectdbcloudConfig;
import com.dtstack.chunjun.connector.selectdbcloud.sink.SelectdbcloudDynamicTableSink;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.utils.TableSchemaUtils;

import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static com.dtstack.chunjun.connector.selectdbcloud.options.SelectdbcloudOptions.*;

/**
 * The {@link SelectdbcloudDynamicTableFactory} translates the catalog table to a table source.
 *
 * <p>Because the table source requires a decoding format, we are discovering the format using the
 * provided {@link FactoryUtil} for convenience.
 */
public final class SelectdbcloudDynamicTableFactory implements DynamicTableSinkFactory {

    public static final String IDENTIFIER = "selectdbcloud-x";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER; // used for matching to `connector = '...'`
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(HOST);
        options.add(HTTP_PORT);
        options.add(QUERY_PORT);
        options.add(CLUSTER_NAME);
        options.add(TABLE_IDENTIFIER);
        options.add(USERNAME);
        options.add(PASSWORD);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();

        options.add(SINK_BUFFER_FLUSH_MAX_ROWS);
        options.add(SINK_MAX_RETRIES);
        options.add(SINK_BUFFER_FLUSH_INTERVAL);
        options.add(SINK_ENABLE_DELETE);
        return options;
    }

    private SelectdbcloudConfig getSelectDBOptions(ReadableConfig readableConfig) {

        SelectdbcloudConfig conf = new SelectdbcloudConfig();

        // set required options
        conf.setHost(readableConfig.get(HOST));
        conf.setHttpPort(readableConfig.get(HTTP_PORT));
        conf.setQueryPort(readableConfig.get(QUERY_PORT));
        conf.setCluster(readableConfig.get(CLUSTER_NAME));
        conf.setUsername(readableConfig.get(USERNAME));
        conf.setPassword(readableConfig.get(PASSWORD));
        conf.setTableIdentifier(readableConfig.get(TABLE_IDENTIFIER));

        // set optional options

        conf.setMaxRetries(readableConfig.get(SINK_MAX_RETRIES));
        conf.setEnableDelete(readableConfig.get(SINK_ENABLE_DELETE));
        conf.setBatchSize(readableConfig.get(SINK_BUFFER_FLUSH_MAX_ROWS));
        conf.setFlushIntervalMills(readableConfig.get(SINK_BUFFER_FLUSH_INTERVAL).toMillis());

        return conf;
    }

    private Properties getStreamLoadProp(Map<String, String> tableOptions) {
        final Properties streamLoadProp = new Properties();

        for (Map.Entry<String, String> entry : tableOptions.entrySet()) {
            if (entry.getKey().startsWith(STAGE_LOAD_PROP_PREFIX)) {
                String subKey = entry.getKey().substring(STAGE_LOAD_PROP_PREFIX.length());
                streamLoadProp.put(subKey, entry.getValue());
            }
        }
        return streamLoadProp;
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        // validate all options
        helper.validateExcept(STAGE_LOAD_PROP_PREFIX);

        SelectdbcloudConfig conf = getSelectDBOptions(helper.getOptions());

        Properties streamLoadProp = getStreamLoadProp(context.getCatalogTable().getOptions());
        conf.setLoadProperties(streamLoadProp);

        TableSchema physicalSchema =
                TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());
        conf.setFieldNames(physicalSchema.getFieldNames());
        conf.setFieldDataTypes(physicalSchema.getFieldDataTypes());

        setDefaults(conf);

        // create and return dynamic table source
        return new SelectdbcloudDynamicTableSink(conf);
    }

    private void setDefaults(SelectdbcloudConfig conf) {

        if (conf.getMaxRetries() == null) {
            conf.setMaxRetries(SINK_MAX_RETRIES.defaultValue());
        }

        if (conf.getBatchSize() < 1) {
            conf.setBatchSize(SINK_BUFFER_FLUSH_MAX_ROWS.defaultValue());
        }

        if (conf.getFlushIntervalMills() < 1L) {
            conf.setFlushIntervalMills(SINK_BUFFER_FLUSH_INTERVAL.defaultValue().toMillis());
        }

        if (conf.getEnableDelete() == null) {
            conf.setEnableDelete(SINK_ENABLE_DELETE.defaultValue());
        }
    }
}
