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

package com.dtstack.chunjun.connector.stream.table;

import com.dtstack.chunjun.connector.stream.conf.StreamConf;
import com.dtstack.chunjun.connector.stream.options.StreamOptions;
import com.dtstack.chunjun.connector.stream.sink.StreamDynamicTableSink;
import com.dtstack.chunjun.connector.stream.source.StreamDynamicTableSource;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.utils.TableSchemaUtils;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * @author chuixue
 * @create 2021-04-08 11:56
 * @description
 */
public class StreamDynamicTableFactory
        implements DynamicTableSinkFactory, DynamicTableSourceFactory {
    public static final String IDENTIFIER = "stream-x";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return new HashSet<>();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(StreamOptions.NUMBER_OF_ROWS);
        options.add(StreamOptions.ROWS_PER_SECOND);
        options.add(StreamOptions.PRINT);
        options.add(StreamOptions.SINK_PARALLELISM);
        return options;
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        // 1.所有的requiredOptions和optionalOptions参数
        final ReadableConfig config = helper.getOptions();

        // 2.参数校验
        helper.validate();

        // 3.封装参数
        StreamConf sinkConf = new StreamConf();
        sinkConf.setPrint(config.get(StreamOptions.PRINT));
        sinkConf.setParallelism(config.get(StreamOptions.SINK_PARALLELISM));

        TableSchema physicalSchema =
                TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());

        return new StreamDynamicTableSink(
                sinkConf,
                context.getCatalogTable().getSchema().toPhysicalRowDataType(),
                physicalSchema);
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        Configuration options = new Configuration();
        context.getCatalogTable().getOptions().forEach(options::setString);
        TableSchema schema =
                TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());

        StreamConf streamConf = new StreamConf();
        streamConf.setSliceRecordCount(
                Collections.singletonList(options.get(StreamOptions.NUMBER_OF_ROWS)));
        streamConf.setPermitsPerSecond(options.get(StreamOptions.ROWS_PER_SECOND));

        return new StreamDynamicTableSource(schema, streamConf);
    }
}
