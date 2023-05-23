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

package com.dtstack.chunjun.connector.hudi.table;

import com.dtstack.chunjun.connector.hudi.utils.HudiConfigUtil;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.table.HoodieTableFactory;
import org.apache.hudi.table.HoodieTableSink;
import org.apache.hudi.table.HoodieTableSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Set;

public class HudiDynamicTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    private static final Logger LOG = LoggerFactory.getLogger(HoodieTableFactory.class);

    public static final String FACTORY_ID = "hudi-x";

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();
        List<String> partitionKeys = context.getCatalogTable().getPartitionKeys();
        Configuration conf = (Configuration) helper.getOptions();

        Path path =
                new Path(
                        conf.getOptional(FlinkOptions.PATH)
                                .orElseThrow(
                                        () ->
                                                new ValidationException(
                                                        "Option [path] should not be empty.")));
        ResolvedSchema schema = context.getCatalogTable().getResolvedSchema();
        HudiConfigUtil.setupTableOptions(conf.getString(FlinkOptions.PATH), conf, schema);
        String keys = getKeys(conf);
        String partitions = getPartitions(conf, context.getCatalogTable());
        HudiConfigUtil.setupConfOptions(
                conf,
                context.getObjectIdentifier().getObjectName(),
                schema,
                context.getObjectIdentifier(),
                keys,
                partitions);
        HudiConfigUtil.sanityCheck(conf, schema);
        return new HoodieTableSource(
                schema,
                path,
                context.getCatalogTable().getPartitionKeys(),
                conf.getString(FlinkOptions.PARTITION_DEFAULT_NAME),
                conf);
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        Configuration conf = FlinkOptions.fromMap(context.getCatalogTable().getOptions());
        ResolvedSchema schema = context.getCatalogTable().getResolvedSchema();
        HudiConfigUtil.setupTableOptions(conf.getString(FlinkOptions.PATH), conf, schema);
        String keys = getKeys(conf);
        String partitions = getPartitions(conf, context.getCatalogTable());
        HudiConfigUtil.setupConfOptions(
                conf,
                context.getObjectIdentifier().getObjectName(),
                schema,
                context.getObjectIdentifier(),
                keys,
                partitions);
        HudiConfigUtil.sanityCheck(conf, schema);
        return new HoodieTableSink(conf, schema);
    }

    @Override
    public String factoryIdentifier() {
        return FACTORY_ID;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Collections.singleton(FlinkOptions.PATH);
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return FlinkOptions.optionalOptions();
    }

    private String getKeys(Configuration conf) {
        String keys = conf.getString(FlinkOptions.RECORD_KEY_FIELD, "");
        if (keys.startsWith(",")) {
            keys = keys.substring(keys.indexOf(",") + 1);
        }
        return keys;
    }

    private String getPartitions(Configuration conf, CatalogTable table) {
        String partitions = conf.getString(FlinkOptions.PARTITION_PATH_FIELD, "");
        if (StringUtils.isBlank(partitions)) {
            List<String> columns = table.getPartitionKeys();
            partitions = String.join(",", columns);
        }
        if (partitions.startsWith(",")) {
            partitions = partitions.substring(partitions.indexOf(",") + 1);
        }
        return partitions;
    }
}
