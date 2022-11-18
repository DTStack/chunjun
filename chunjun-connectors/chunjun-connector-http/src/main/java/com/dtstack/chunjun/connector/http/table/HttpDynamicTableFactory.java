/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dtstack.chunjun.connector.http.table;

import com.dtstack.chunjun.config.FieldConfig;
import com.dtstack.chunjun.connector.http.common.HttpRestConfig;
import com.dtstack.chunjun.connector.http.common.HttpWriterConfig;
import com.dtstack.chunjun.connector.http.common.MetaParam;
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

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class HttpDynamicTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {
    public static final String IDENTIFIER = "http-x";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(HttpOptions.URL);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(HttpOptions.DECODE);
        options.add(HttpOptions.METHOD);
        options.add(HttpOptions.HEADER);
        options.add(HttpOptions.BODY);
        options.add(HttpOptions.PARAMS);
        options.add(HttpOptions.INTERVALTIME);
        options.add(HttpOptions.COLUMN);
        options.add(HttpOptions.DELAY);
        options.add(HttpOptions.DATA_SUBJECT);
        options.add(HttpOptions.CYCLES);

        return options;
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        // 1.所有的requiredOptions和optionalOptions参数
        final ReadableConfig config = helper.getOptions();

        // 2.参数校验
        helper.validate();

        // 3.封装参数
        TableSchema physicalSchema =
                TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());
        HttpWriterConfig restapiWriterConf = getRestapiWriterConf(config);

        return new HttpDynamicTableSink(physicalSchema, restapiWriterConf);
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
        TableSchema physicalSchema =
                TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());
        HttpRestConfig httpRestConfig = getRestapiConf(config);

        return new HttpDynamicTableSource(physicalSchema, httpRestConfig);
    }

    private HttpWriterConfig getRestapiWriterConf(ReadableConfig config) {
        Gson gson = GsonUtil.setTypeAdapter(new Gson());
        HttpWriterConfig httpWriterConfig = new HttpWriterConfig();
        httpWriterConfig.setUrl(config.get(HttpOptions.URL));
        httpWriterConfig.setMethod(config.get(HttpOptions.METHOD));
        httpWriterConfig.setHeader(
                gson.fromJson(
                        config.get(HttpOptions.HEADER),
                        new TypeToken<Map<String, String>>() {}.getType()));
        httpWriterConfig.setColumn(
                gson.fromJson(
                        config.get(HttpOptions.COLUMN),
                        new TypeToken<List<FieldConfig>>() {}.getType()));
        httpWriterConfig.setDelay(config.get(HttpOptions.DELAY));
        return httpWriterConfig;
    }

    /**
     * 初始化RestapiConf
     *
     * @param config RestapiConf
     * @return
     */
    private HttpRestConfig getRestapiConf(ReadableConfig config) {
        Gson gson = GsonUtil.setTypeAdapter(new Gson());
        HttpRestConfig httpRestConfig = new HttpRestConfig();
        httpRestConfig.setIntervalTime(config.get(HttpOptions.INTERVALTIME));
        httpRestConfig.setUrl(config.get(HttpOptions.URL));
        httpRestConfig.setDecode(config.get(HttpOptions.DECODE));
        httpRestConfig.setRequestMode(config.get(HttpOptions.METHOD));
        httpRestConfig.setDataSubject(config.get(HttpOptions.DATA_SUBJECT));
        httpRestConfig.setCycles(config.get(HttpOptions.CYCLES));
        httpRestConfig.setParam(
                gson.fromJson(
                        config.get(HttpOptions.PARAMS),
                        new TypeToken<List<MetaParam>>() {}.getType()));
        httpRestConfig.setHeader(
                gson.fromJson(
                        config.get(HttpOptions.HEADER),
                        new TypeToken<List<MetaParam>>() {}.getType()));
        httpRestConfig.setBody(
                gson.fromJson(
                        config.get(HttpOptions.BODY),
                        new TypeToken<List<MetaParam>>() {}.getType()));
        httpRestConfig.setColumn(
                gson.fromJson(
                        config.get(HttpOptions.COLUMN),
                        new TypeToken<List<FieldConfig>>() {}.getType()));
        return httpRestConfig;
    }
}
