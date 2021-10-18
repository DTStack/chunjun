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

package com.dtstack.flinkx.connector.solr.table;

import com.dtstack.flinkx.connector.solr.SolrConf;
import com.dtstack.flinkx.security.KerberosConfig;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.utils.TableSchemaUtils;

import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.dtstack.flinkx.connector.solr.table.SolrClientOptions.COLLECTION;
import static com.dtstack.flinkx.connector.solr.table.SolrClientOptions.ZK_CHROOT;
import static com.dtstack.flinkx.connector.solr.table.SolrClientOptions.ZK_HOSTS;
import static com.dtstack.flinkx.security.KerberosOptions.KEYTAB;
import static com.dtstack.flinkx.security.KerberosOptions.KRB5_CONF;
import static com.dtstack.flinkx.security.KerberosOptions.PRINCIPAL;
import static com.dtstack.flinkx.table.options.SinkOptions.SINK_BUFFER_FLUSH_INTERVAL;
import static com.dtstack.flinkx.table.options.SinkOptions.SINK_BUFFER_FLUSH_MAX_ROWS;
import static org.apache.flink.table.factories.FactoryUtil.SINK_PARALLELISM;

/**
 * @author wuren
 * @program flinkx
 * @create 2021/05/31
 */
public class SolrDynamicTableFactory implements DynamicTableSinkFactory {

    private static final String IDENTIFIER = "solr-x";

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        final ReadableConfig config = helper.getOptions();

        helper.validate();

        TableSchema physicalSchema =
                TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());
        SolrConf solrConf = new SolrConf();
        List<String> zkHosts = Arrays.asList(StringUtils.split(config.get(ZK_HOSTS), ','));
        solrConf.setZkHosts(zkHosts);
        solrConf.setZkChroot(config.get(ZK_CHROOT));
        solrConf.setCollection(config.get(COLLECTION));

        solrConf.setParallelism(config.get(SINK_PARALLELISM));
        KerberosConfig kerberosConfig =
                new KerberosConfig(
                        config.get(PRINCIPAL), config.get(KEYTAB), config.get(KRB5_CONF));
        solrConf.setKerberosConfig(kerberosConfig);

        solrConf.setBatchSize(config.get(SINK_BUFFER_FLUSH_MAX_ROWS));
        solrConf.setFlushIntervalMills(config.get(SINK_BUFFER_FLUSH_INTERVAL));

        return new SolrDynamicTableSink(solrConf, physicalSchema);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> requiredOptions = new HashSet<>();
        requiredOptions.add(ZK_HOSTS);
        requiredOptions.add(COLLECTION);
        return requiredOptions;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> optionalOptions = new HashSet<>();
        optionalOptions.add(ZK_CHROOT);

        optionalOptions.add(PRINCIPAL);
        optionalOptions.add(KEYTAB);
        optionalOptions.add(KRB5_CONF);

        optionalOptions.add(SINK_BUFFER_FLUSH_MAX_ROWS);
        optionalOptions.add(SINK_BUFFER_FLUSH_INTERVAL);
        optionalOptions.add(SINK_PARALLELISM);

        return optionalOptions;
    }
}
