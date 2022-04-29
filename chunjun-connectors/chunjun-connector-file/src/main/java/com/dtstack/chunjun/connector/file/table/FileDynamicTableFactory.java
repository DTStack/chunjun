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

import com.dtstack.chunjun.conf.BaseFileConf;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.utils.TableSchemaUtils;

import java.util.HashSet;
import java.util.Set;

/**
 * @program: flinkx
 * @author: xiuzhu
 * @create: 2021/06/24
 */
public class FileDynamicTableFactory implements DynamicTableSourceFactory {

    /** 通过该值查找具体插件 */
    private static final String IDENTIFIER = "file-x";

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {

        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        final ReadableConfig config = helper.getOptions();

        TableSchema physicalSchema =
                TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());

        final DecodingFormat<DeserializationSchema<RowData>> decodingFormat =
                getDecodingFormat(helper);

        BaseFileConf fileConf = getFileConfByOptions(config);
        return new FileDynamicTableSource(physicalSchema, fileConf, decodingFormat);
    }

    private BaseFileConf getFileConfByOptions(ReadableConfig config) {
        BaseFileConf fileConf = new BaseFileConf();
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
