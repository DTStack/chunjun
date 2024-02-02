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

package com.dtstack.chunjun.connector.s3.table;

import com.dtstack.chunjun.connector.s3.config.S3Config;
import com.dtstack.chunjun.connector.s3.sink.S3DynamicTableSink;
import com.dtstack.chunjun.connector.s3.source.S3DynamicTableSource;
import com.dtstack.chunjun.connector.s3.table.options.S3Options;
import com.dtstack.chunjun.table.options.SinkOptions;
import com.dtstack.chunjun.util.GsonUtil;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;

import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.Set;

public class S3DynamicTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {
    private static final String IDENTIFIER = "s3-x";

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

        helper.validate();

        ReadableConfig options = helper.getOptions();

        S3Config s3Config = new S3Config();
        s3Config.setAccessKey(options.get(S3Options.ACCESS_Key));
        s3Config.setSecretKey(options.get(S3Options.SECRET_Key));
        s3Config.setBucket(options.get(S3Options.BUCKET));
        s3Config.setObjects(
                GsonUtil.GSON.fromJson(options.get(S3Options.OBJECTS), ArrayList.class));
        s3Config.setFieldDelimiter(options.get(S3Options.FIELD_DELIMITER).trim().toCharArray()[0]);
        s3Config.setEncoding(options.get(S3Options.ENCODING));
        s3Config.setRegion(options.get(S3Options.REGION));
        s3Config.setFirstLineHeader(options.get(S3Options.IS_FIRST_LINE_HEADER));
        s3Config.setEndpoint(options.get(S3Options.ENDPOINT));
        s3Config.setCompress(options.get(S3Options.COMPRESS));
        return new S3DynamicTableSource(context.getCatalogTable().getResolvedSchema(), s3Config);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = Sets.newHashSet();
        options.add(S3Options.ACCESS_Key);
        options.add(S3Options.SECRET_Key);
        options.add(S3Options.BUCKET);
        options.add(S3Options.FIELD_DELIMITER);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = Sets.newHashSet();
        options.add(S3Options.ENCODING);
        options.add(S3Options.REGION);
        options.add(S3Options.IS_FIRST_LINE_HEADER);
        options.add(S3Options.OBJECTS);
        options.add(S3Options.OBJECT);
        options.add(S3Options.ENDPOINT);
        options.add(S3Options.COMPRESS);
        options.add(S3Options.WRITE_SINGLE_OBJECT);
        options.add(S3Options.USE_V2);
        options.add(S3Options.SUFFIX);
        options.add(SinkOptions.SINK_PARALLELISM);
        options.add(S3Options.WRITE_MODE);
        return options;
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

        helper.validate();

        ReadableConfig options = helper.getOptions();

        S3Config s3Config = new S3Config();
        s3Config.setAccessKey(options.get(S3Options.ACCESS_Key));
        s3Config.setSecretKey(options.get(S3Options.SECRET_Key));
        s3Config.setBucket(options.get(S3Options.BUCKET));
        s3Config.setObject(options.get(S3Options.OBJECT));
        s3Config.setFieldDelimiter(options.get(S3Options.FIELD_DELIMITER).trim().toCharArray()[0]);
        s3Config.setEncoding(options.get(S3Options.ENCODING));
        s3Config.setRegion(options.get(S3Options.REGION));
        s3Config.setFirstLineHeader(options.get(S3Options.IS_FIRST_LINE_HEADER));
        s3Config.setEndpoint(options.get(S3Options.ENDPOINT));
        s3Config.setCompress(options.get(S3Options.COMPRESS));
        s3Config.setWriteSingleObject(options.get(S3Options.WRITE_SINGLE_OBJECT));
        s3Config.setUseV2(options.get(S3Options.USE_V2));
        s3Config.setSuffix(options.get(S3Options.SUFFIX));
        s3Config.setParallelism(options.get(SinkOptions.SINK_PARALLELISM));
        s3Config.setWriteMode(options.get(S3Options.WRITE_MODE));

        return new S3DynamicTableSink(context.getCatalogTable().getResolvedSchema(), s3Config);
    }
}
