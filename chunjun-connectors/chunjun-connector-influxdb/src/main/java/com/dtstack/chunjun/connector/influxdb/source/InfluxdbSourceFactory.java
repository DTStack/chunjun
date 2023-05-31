/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.dtstack.chunjun.connector.influxdb.source;

import com.dtstack.chunjun.config.SyncConfig;
import com.dtstack.chunjun.connector.influxdb.config.InfluxdbSourceConfig;
import com.dtstack.chunjun.connector.influxdb.converter.InfluxdbRawTypeMapper;
import com.dtstack.chunjun.converter.RawTypeMapper;
import com.dtstack.chunjun.source.SourceFactory;
import com.dtstack.chunjun.util.GsonUtil;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;

import com.google.gson.ExclusionStrategy;
import com.google.gson.FieldAttributes;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.util.Map;

public class InfluxdbSourceFactory extends SourceFactory {

    private final InfluxdbSourceConfig config;

    public InfluxdbSourceFactory(SyncConfig syncConfig, StreamExecutionEnvironment env) {
        super(syncConfig, env);
        Map<String, Object> parameter = syncConfig.getJob().getReader().getParameter();
        Gson gson =
                new GsonBuilder()
                        .addDeserializationExclusionStrategy(
                                new ExclusionStrategy() {
                                    @Override
                                    public boolean shouldSkipField(FieldAttributes f) {
                                        // support column["id", "name"...] and  ["*"],
                                        // column do not deserialize.
                                        return "column".equals(f.getName());
                                    }

                                    @Override
                                    public boolean shouldSkipClass(Class<?> clazz) {
                                        return false;
                                    }
                                })
                        .create();
        GsonUtil.setTypeAdapter(gson);
        this.config = gson.fromJson(gson.toJson(parameter), InfluxdbSourceConfig.class);
        config.setColumn(syncConfig.getReader().getFieldList());
        super.initCommonConf(config);
    }

    @Override
    public RawTypeMapper getRawTypeMapper() {
        return InfluxdbRawTypeMapper::apply;
    }

    @Override
    public DataStream<RowData> createSource() {
        InfluxdbInputFormatBuilder builder = new InfluxdbInputFormatBuilder();
        builder.setInfluxdbConfig(config);
        builder.setRowConverter(null, useAbstractBaseColumn);
        return createInput(builder.finish());
    }
}
