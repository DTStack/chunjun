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
package com.dtstack.flinkx.connector.restapi.table;

import com.dtstack.flinkx.conf.FieldConf;
import com.dtstack.flinkx.connector.restapi.common.HttpRestConfig;
import com.dtstack.flinkx.connector.restapi.common.MetaParam;
import com.dtstack.flinkx.connector.restapi.options.RestapiOptions;
import com.dtstack.flinkx.connector.restapi.source.RestapiDynamicTableSource;
import com.dtstack.flinkx.util.GsonUtil;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.utils.TableSchemaUtils;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Date: 2021/04/27 Company: www.dtstack.com
 *
 * @author shifang
 */
public class RestapiDynamicTableFactory implements DynamicTableSourceFactory {
    public static final String IDENTIFIER = "restapi-x";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(RestapiOptions.URL);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(RestapiOptions.DECODE);
        options.add(RestapiOptions.REQUESTMODE);
        options.add(RestapiOptions.HEADER);
        options.add(RestapiOptions.BODY);
        options.add(RestapiOptions.PARAMS);
        options.add(RestapiOptions.INTERVALTIME);
        options.add(RestapiOptions.COLUMN);

        return options;
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

        return new RestapiDynamicTableSource(physicalSchema, httpRestConfig);
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
        httpRestConfig.setIntervalTime(config.get(RestapiOptions.INTERVALTIME));
        httpRestConfig.setUrl(config.get(RestapiOptions.URL));
        httpRestConfig.setDecode(config.get(RestapiOptions.DECODE));
        httpRestConfig.setRequestMode(config.get(RestapiOptions.REQUESTMODE));
        httpRestConfig.setParam(
                gson.fromJson(
                        config.get(RestapiOptions.PARAMS),
                        new TypeToken<List<MetaParam>>() {}.getType()));
        httpRestConfig.setHeader(
                gson.fromJson(
                        config.get(RestapiOptions.HEADER),
                        new TypeToken<List<MetaParam>>() {}.getType()));
        httpRestConfig.setBody(
                gson.fromJson(
                        config.get(RestapiOptions.BODY),
                        new TypeToken<List<MetaParam>>() {}.getType()));
        httpRestConfig.setColumn(
                gson.fromJson(
                        config.get(RestapiOptions.COLUMN),
                        new TypeToken<List<FieldConf>>() {}.getType()));
        return httpRestConfig;
    }
}
