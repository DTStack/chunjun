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

package com.dtstack.chunjun.connector.emqx.table;

import com.dtstack.chunjun.connector.emqx.config.EmqxConfig;
import com.dtstack.chunjun.connector.emqx.sink.EmqxDynamicTableSink;
import com.dtstack.chunjun.connector.emqx.source.EmqxDynamicTableSource;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import static com.dtstack.chunjun.connector.emqx.options.EmqxOptions.BROKER;
import static com.dtstack.chunjun.connector.emqx.options.EmqxOptions.FORMAT;
import static com.dtstack.chunjun.connector.emqx.options.EmqxOptions.ISCLEANSESSION;
import static com.dtstack.chunjun.connector.emqx.options.EmqxOptions.PASSWORD;
import static com.dtstack.chunjun.connector.emqx.options.EmqxOptions.QOS;
import static com.dtstack.chunjun.connector.emqx.options.EmqxOptions.TOPIC;
import static com.dtstack.chunjun.connector.emqx.options.EmqxOptions.USERNAME;
import static com.dtstack.chunjun.connector.emqx.options.EmqxOptions.connectRetryTimes;

public class EmqxDynamicTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    /** 通过该值查找具体插件 */
    private static final String IDENTIFIER = "emqx-x";

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        // 1.所有的requiredOptions和optionalOptions参数
        final ReadableConfig config = helper.getOptions();

        // 2.参数校验
        helper.validate();

        // 3.封装参数
        ResolvedSchema resolvedSchema = context.getCatalogTable().getResolvedSchema();
        final EncodingFormat<SerializationSchema<RowData>> valueEncodingFormat =
                getValueEncodingFormat(helper);

        return new EmqxDynamicTableSink(
                resolvedSchema,
                getEmqxConf(config),
                new EncodingFormatWrapper(valueEncodingFormat));
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        // 1.所有的requiredOptions和optionalOptions参数
        final ReadableConfig config = helper.getOptions();

        // 2.参数校验
        helper.validate();

        // 3.封装参数
        ResolvedSchema resolvedSchema = context.getCatalogTable().getResolvedSchema();
        // 加载json format，目前只支持json格式
        final DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat =
                getValueDecodingFormat(helper);

        return new EmqxDynamicTableSource(resolvedSchema, getEmqxConf(config), valueDecodingFormat);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> requiredOptions = new HashSet<>();
        requiredOptions.add(BROKER);
        requiredOptions.add(TOPIC);
        return requiredOptions;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> optionalOptions = new HashSet<>();
        optionalOptions.add(ISCLEANSESSION);
        optionalOptions.add(QOS);
        optionalOptions.add(USERNAME);
        optionalOptions.add(PASSWORD);
        optionalOptions.add(FORMAT);
        optionalOptions.add(connectRetryTimes);
        return optionalOptions;
    }

    private EmqxConfig getEmqxConf(ReadableConfig readableConfig) {
        EmqxConfig emqxConfig = new EmqxConfig();
        emqxConfig.setBroker(readableConfig.get(BROKER));
        emqxConfig.setTopic(readableConfig.get(TOPIC));
        emqxConfig.setUsername(readableConfig.get(USERNAME));
        emqxConfig.setPassword(readableConfig.get(PASSWORD));
        emqxConfig.setCleanSession(readableConfig.get(ISCLEANSESSION));
        emqxConfig.setQos(readableConfig.get(QOS));
        return emqxConfig;
    }

    private static DecodingFormat<DeserializationSchema<RowData>> getValueDecodingFormat(
            FactoryUtil.TableFactoryHelper helper) {
        return helper.discoverOptionalDecodingFormat(DeserializationFormatFactory.class, FORMAT)
                .orElseGet(
                        () ->
                                helper.discoverDecodingFormat(
                                        DeserializationFormatFactory.class, FORMAT));
    }

    private static EncodingFormat<SerializationSchema<RowData>> getValueEncodingFormat(
            FactoryUtil.TableFactoryHelper helper) {
        return helper.discoverOptionalEncodingFormat(SerializationFormatFactory.class, FORMAT)
                .orElseGet(
                        () ->
                                helper.discoverEncodingFormat(
                                        SerializationFormatFactory.class, FORMAT));
    }

    /**
     * It is used to wrap the encoding format and expose the desired changelog mode. It's only works
     * for insert-only format.
     */
    protected static class EncodingFormatWrapper
            implements EncodingFormat<SerializationSchema<RowData>> {
        private final EncodingFormat<SerializationSchema<RowData>> innerEncodingFormat;

        public static final ChangelogMode SINK_CHANGELOG_MODE =
                ChangelogMode.newBuilder()
                        .addContainedKind(RowKind.INSERT)
                        .addContainedKind(RowKind.UPDATE_AFTER)
                        .addContainedKind(RowKind.DELETE)
                        .build();

        public EncodingFormatWrapper(
                EncodingFormat<SerializationSchema<RowData>> innerEncodingFormat) {
            this.innerEncodingFormat = innerEncodingFormat;
        }

        @Override
        public SerializationSchema<RowData> createRuntimeEncoder(
                DynamicTableSink.Context context, DataType consumedDataType) {
            return innerEncodingFormat.createRuntimeEncoder(context, consumedDataType);
        }

        @Override
        public ChangelogMode getChangelogMode() {
            return SINK_CHANGELOG_MODE;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) {
                return true;
            }

            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }

            EncodingFormatWrapper that = (EncodingFormatWrapper) obj;
            return Objects.equals(innerEncodingFormat, that.innerEncodingFormat);
        }

        @Override
        public int hashCode() {
            return Objects.hash(innerEncodingFormat);
        }
    }
}
