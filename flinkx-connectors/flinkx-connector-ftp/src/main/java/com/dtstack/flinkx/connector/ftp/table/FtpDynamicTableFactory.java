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

package com.dtstack.flinkx.connector.ftp.table;

import com.dtstack.flinkx.connector.ftp.conf.FtpConfig;
import com.dtstack.flinkx.connector.ftp.options.FtpOptions;
import com.dtstack.flinkx.connector.ftp.sink.FtpDynamicTableSink;
import com.dtstack.flinkx.connector.ftp.source.FtpDynamicTableSource;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
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
import org.apache.flink.table.utils.TableSchemaUtils;

import java.util.HashSet;
import java.util.Set;

/**
 * @program: flinkx
 * @author: xiuzhu
 * @create: 2021/06/19
 */
public class FtpDynamicTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    private static final String IDENTIFIER = "ftp-x";

    private static FtpConfig getFtpConfByOptions(ReadableConfig config) {
        FtpConfig ftpConfig = new FtpConfig();
        ftpConfig.setPath(config.get(FtpOptions.PATH));
        ftpConfig.setHost(config.get(FtpOptions.HOST));
        ftpConfig.setProtocol(config.get(FtpOptions.PROTOCOL));
        ftpConfig.setUsername(config.get(FtpOptions.USERNAME));
        ftpConfig.setPassword(config.get(FtpOptions.PASSWORD));

        ftpConfig.setEncoding(config.get(FtpOptions.ENCODING));

        if (config.get(FtpOptions.TIMEOUT) != null) {
            ftpConfig.setTimeout(config.get(FtpOptions.TIMEOUT));
        }

        if (config.get(FtpOptions.CONNECT_PATTERN) != null) {
            ftpConfig.setConnectPattern(config.get(FtpOptions.CONNECT_PATTERN));
        }

        if (config.get(FtpOptions.PORT) == null) {
            ftpConfig.setDefaultPort();
        } else {
            ftpConfig.setPort(config.get(FtpOptions.PORT));
        }

        return ftpConfig;
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {

        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        final ReadableConfig config = helper.getOptions();
        helper.validate();

        TableSchema physicalSchema =
                TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());

        DecodingFormat<DeserializationSchema<RowData>> decodingFormat =
                helper.discoverDecodingFormat(
                        DeserializationFormatFactory.class, FtpOptions.FORMAT);

        FtpConfig ftpConfig = getFtpConfByOptions(config);

        return new FtpDynamicTableSource(physicalSchema, ftpConfig, decodingFormat);
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        final ReadableConfig config = helper.getOptions();
        helper.validate();

        TableSchema physicalSchema =
                TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());

        EncodingFormat<SerializationSchema<RowData>> valueEncodingFormat =
                helper.discoverOptionalEncodingFormat(
                                SerializationFormatFactory.class, FtpOptions.FORMAT)
                        .orElseGet(
                                () ->
                                        helper.discoverEncodingFormat(
                                                SerializationFormatFactory.class,
                                                FtpOptions.FORMAT));

        FtpConfig ftpConfig = getFtpConfByOptions(config);

        return new FtpDynamicTableSink(physicalSchema, ftpConfig, valueEncodingFormat);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(FtpOptions.PATH);
        options.add(FtpOptions.PROTOCOL);
        options.add(FtpOptions.HOST);
        options.add(FtpOptions.USERNAME);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(FtpOptions.FILE_NAME);
        options.add(FtpOptions.PASSWORD);
        options.add(FtpOptions.TIMEOUT);
        options.add(FtpOptions.ENCODING);
        options.add(FtpOptions.MAX_FILE_SIZE);
        options.add(FtpOptions.FORMAT);
        return options;
    }
}
