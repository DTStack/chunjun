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

package com.dtstack.flinkx.connector.stream.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.utils.TableSchemaUtils;

import com.dtstack.flinkx.connector.stream.conf.StreamLookupConf;
import com.dtstack.flinkx.connector.stream.conf.StreamSinkConf;
import com.dtstack.flinkx.connector.stream.sink.StreamDynamicTableSink;
import com.dtstack.flinkx.connector.stream.source.StreamDynamicTableSource;
import com.dtstack.flinkx.constants.LookupConstant;

import java.util.HashSet;
import java.util.Set;

import static com.dtstack.flinkx.connector.stream.constants.StreamConstants.PRINT_IDENTIFIER;
import static com.dtstack.flinkx.connector.stream.constants.StreamConstants.STANDARD_ERROR;

/**
 * @author chuixue
 * @create 2021-04-08 11:56
 * @description
 **/
public class StreamDynamicTableFactory implements DynamicTableSinkFactory, DynamicTableSourceFactory {
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
        Set<ConfigOption<?>> optionalOptions = new HashSet<>();
        optionalOptions.add(LookupConstant.CACHE);
        return optionalOptions;
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        // 1.所有的requiredOptions和optionalOptions参数
        final ReadableConfig config = helper.getOptions();

        // 2.参数校验
        helper.validate();
        validateConfigOptions(config);

        // 3.封装参数
        StreamSinkConf sinkConf = StreamSinkConf
                .builder()
                .setPrintIdentifier(config.get(PRINT_IDENTIFIER))
                .setStdErr(config.get(STANDARD_ERROR));

        TableSchema physicalSchema =
                TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());

        return new StreamDynamicTableSink(
                sinkConf,
                context.getCatalogTable().getSchema().toPhysicalRowDataType(),
                physicalSchema);
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        // 1.所有的requiredOptions和optionalOptions参数
        final ReadableConfig config = helper.getOptions();

        // 2.参数校验
        helper.validate();
        validateConfigOptions(config);

        // 3.封装参数
        StreamLookupConf lookupConf = (StreamLookupConf) StreamLookupConf
                .build()
                .setTableName(context.getObjectIdentifier().getObjectName())
                .setCache(config.get(LookupConstant.CACHE))
                .setCacheSize(1000L)
                .setPeriod(3600 * 1000L);
        TableSchema physicalSchema =
                TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());

        return new StreamDynamicTableSource(lookupConf, physicalSchema);
    }

    /**
     * 参数校验，如：必填参数不能空、格式必须对，可选参数如果填了格式必须对、大小范围不能越界等
     *
     * @param config
     */
    private void validateConfigOptions(ReadableConfig config) {
        // 抽出一个工具类，和BaseRichInputFormatBuilder中的finish都是校验参数
    }
}
