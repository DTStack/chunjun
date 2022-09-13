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

import com.dtstack.chunjun.connector.s3.conf.S3Conf;
import com.dtstack.chunjun.connector.s3.sink.S3DynamicTableSink;
import com.dtstack.chunjun.connector.s3.source.S3DynamicTableSource;
import com.dtstack.chunjun.connector.s3.table.options.S3Options;
import com.dtstack.chunjun.util.GsonUtil;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.utils.TableSchemaUtils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

public class S3DynamicTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {
    private static final String IDENTIFIER = "s3-x";

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

        helper.validate();

        ReadableConfig options = helper.getOptions();

        S3Conf s3Conf = new S3Conf();
        s3Conf.setAccessKey(options.get(S3Options.ACCESS_Key));
        s3Conf.setSecretKey(options.get(S3Options.SECRET_Key));
        s3Conf.setBucket(options.get(S3Options.BUCKET));
        s3Conf.setObjects(GsonUtil.GSON.fromJson(options.get(S3Options.OBJECTS), ArrayList.class));
        s3Conf.setFieldDelimiter(options.get(S3Options.FIELD_DELIMITER).trim().toCharArray()[0]);
        s3Conf.setEncoding(options.get(S3Options.ENCODING));
        s3Conf.setRegion(options.get(S3Options.REGION));
        s3Conf.setIsFirstLineHeader(options.get(S3Options.IS_FIRST_LINE_HEADER));

        TableSchema physicalSchema =
                TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());

        return new S3DynamicTableSource(physicalSchema, s3Conf);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet();
        options.add(S3Options.ACCESS_Key);
        options.add(S3Options.SECRET_Key);
        options.add(S3Options.BUCKET);
        options.add(S3Options.FIELD_DELIMITER);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet();
        options.add(S3Options.ENCODING);
        options.add(S3Options.REGION);
        options.add(S3Options.IS_FIRST_LINE_HEADER);
        options.add(S3Options.OBJECTS);
        options.add(S3Options.OBJECT);
        return options;
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

        helper.validate();

        ReadableConfig options = helper.getOptions();

        S3Conf s3Conf = new S3Conf();
        s3Conf.setAccessKey(options.get(S3Options.ACCESS_Key));
        s3Conf.setSecretKey(options.get(S3Options.SECRET_Key));
        s3Conf.setBucket(options.get(S3Options.BUCKET));
        s3Conf.setObject(options.get(S3Options.OBJECT));
        s3Conf.setFieldDelimiter(options.get(S3Options.FIELD_DELIMITER).trim().toCharArray()[0]);
        s3Conf.setEncoding(options.get(S3Options.ENCODING));
        s3Conf.setRegion(options.get(S3Options.REGION));
        s3Conf.setIsFirstLineHeader(options.get(S3Options.IS_FIRST_LINE_HEADER));

        TableSchema physicalSchema =
                TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());

        return new S3DynamicTableSink(physicalSchema, s3Conf);
    }
}
