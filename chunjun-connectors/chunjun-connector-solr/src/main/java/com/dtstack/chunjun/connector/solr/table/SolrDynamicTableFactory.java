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

package com.dtstack.chunjun.connector.solr.table;

import com.dtstack.chunjun.connector.solr.SolrConfig;
import com.dtstack.chunjun.security.KerberosConfig;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;

import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.dtstack.chunjun.connector.solr.table.SolrClientOptions.COLLECTION;
import static com.dtstack.chunjun.connector.solr.table.SolrClientOptions.ZK_CHROOT;
import static com.dtstack.chunjun.connector.solr.table.SolrClientOptions.ZK_HOSTS;
import static com.dtstack.chunjun.security.KerberosOptions.KEYTAB;
import static com.dtstack.chunjun.security.KerberosOptions.KRB5_CONF;
import static com.dtstack.chunjun.security.KerberosOptions.PRINCIPAL;
import static com.dtstack.chunjun.table.options.SinkOptions.SINK_BUFFER_FLUSH_INTERVAL;
import static com.dtstack.chunjun.table.options.SinkOptions.SINK_BUFFER_FLUSH_MAX_ROWS;
import static org.apache.flink.table.factories.FactoryUtil.SINK_PARALLELISM;

public class SolrDynamicTableFactory implements DynamicTableSinkFactory {

    private static final String IDENTIFIER = "solr-x";

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        final ReadableConfig config = helper.getOptions();

        helper.validate();

        SolrConfig solrConfig = new SolrConfig();
        List<String> zkHosts = Arrays.asList(StringUtils.split(config.get(ZK_HOSTS), ','));
        solrConfig.setZkHosts(zkHosts);
        solrConfig.setZkChroot(config.get(ZK_CHROOT));
        solrConfig.setCollection(config.get(COLLECTION));

        solrConfig.setParallelism(config.get(SINK_PARALLELISM));
        KerberosConfig kerberosConfig =
                new KerberosConfig(
                        config.get(PRINCIPAL), config.get(KEYTAB), config.get(KRB5_CONF));
        solrConfig.setKerberosConfig(kerberosConfig);

        solrConfig.setBatchSize(config.get(SINK_BUFFER_FLUSH_MAX_ROWS));
        solrConfig.setFlushIntervalMills(config.get(SINK_BUFFER_FLUSH_INTERVAL));

        return new SolrDynamicTableSink(solrConfig, context.getCatalogTable().getResolvedSchema());
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
