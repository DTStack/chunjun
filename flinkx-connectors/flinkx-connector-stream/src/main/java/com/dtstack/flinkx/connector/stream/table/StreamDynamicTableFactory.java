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
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.util.Preconditions;

import com.dtstack.flinkx.connector.stream.conf.StreamSinkConf;
import com.dtstack.flinkx.connector.stream.sink.StreamDynamicTableSink;
import com.dtstack.flinkx.connector.stream.source.StreamDynamicTableSource;

import java.util.Arrays;
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
    public static final String IDENTIFIER = "stream";

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
        return new HashSet<>();
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
        StreamSinkConf streamSinkConf = StreamSinkConf
                .builder()
                .setType(context.getCatalogTable().getSchema().toPhysicalRowDataType())
                .setPrintIdentifier(config.get(PRINT_IDENTIFIER))
                .setStdErr(config.get(STANDARD_ERROR));

        return new StreamDynamicTableSink(streamSinkConf);
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        final ReadableConfig config = helper.getOptions();

        helper.validate();
        validateConfigOptions(config);
        TableSchema physicalSchema =
                TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());

        return new StreamDynamicTableSource(physicalSchema);
    }

    /**
     * 参数校验，如：必填参数不能空、格式必须对，可选参数如果填了格式必须对、大小范围不能越界等
     *
     * @param config
     */
    private void validateConfigOptions(ReadableConfig config) {
        checkAllOrNone(config, new ConfigOption[]{
                ConfigOptions.key("username")
                        .stringType()
                        .noDefaultValue(),
                ConfigOptions.key("password")
                        .stringType()
                        .noDefaultValue()
        });
    }

    private void checkAllOrNone(ReadableConfig config, ConfigOption<?>[] configOptions) {
        int presentCount = 0;
        for (ConfigOption configOption : configOptions) {
            if (config.getOptional(configOption).isPresent()) {
                presentCount++;
            }
        }
        String[] propertyNames =
                Arrays.stream(configOptions).map(ConfigOption::key).toArray(String[]::new);
        Preconditions.checkArgument(
                configOptions.length == presentCount || presentCount == 0,
                "Either all or none of the following options should be provided:\n"
                        + String.join("\n", propertyNames));
    }
}
