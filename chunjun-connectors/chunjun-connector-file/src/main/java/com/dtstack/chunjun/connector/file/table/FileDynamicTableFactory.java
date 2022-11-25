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

package com.dtstack.chunjun.connector.file.table;

import com.dtstack.chunjun.config.BaseFileConfig;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.HashSet;
import java.util.Set;

public class FileDynamicTableFactory implements DynamicTableSourceFactory {

    /** 通过该值查找具体插件 */
    private static final String IDENTIFIER = "file-x";

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {

        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        final ReadableConfig config = helper.getOptions();

        ResolvedSchema resolvedSchema = context.getCatalogTable().getResolvedSchema();

        final DecodingFormat<DeserializationSchema<RowData>> decodingFormat =
                getDecodingFormat(helper);

        BaseFileConfig fileConf = getFileConfByOptions(config);
        return new FileDynamicTableSource(resolvedSchema, fileConf, decodingFormat);
    }

    private BaseFileConfig getFileConfByOptions(ReadableConfig config) {
        BaseFileConfig fileConf = new BaseFileConfig();
        fileConf.setPath(config.get(FileOptions.PATH));
        fileConf.setEncoding(config.get(FileOptions.ENCODING));
        fileConf.setFromLine(config.get(FileOptions.SCAN_LINE));
        return fileConf;
    }

    private DecodingFormat<DeserializationSchema<RowData>> getDecodingFormat(
            FactoryUtil.TableFactoryHelper helper) {
        return helper.discoverDecodingFormat(
                DeserializationFormatFactory.class, FileOptions.FORMAT);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(FileOptions.PATH);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(FileOptions.FORMAT);
        options.add(FileOptions.ENCODING);
        return options;
    }
}
