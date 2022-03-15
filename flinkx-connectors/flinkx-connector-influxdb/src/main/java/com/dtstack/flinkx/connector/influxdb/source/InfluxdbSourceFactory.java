/*
 *
 *  *
 *  *  * Licensed to the Apache Software Foundation (ASF) under one
 *  *  * or more contributor license agreements.  See the NOTICE file
 *  *  * distributed with this work for additional information
 *  *  * regarding copyright ownership.  The ASF licenses this file
 *  *  * to you under the Apache License, Version 2.0 (the
 *  *  * "License"); you may not use this file except in compliance
 *  *  * with the License.  You may obtain a copy of the License at
 *  *  *
 *  *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *  *
 *  *  * Unless required by applicable law or agreed to in writing, software
 *  *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  * See the License for the specific language governing permissions and
 *  *  * limitations under the License.
 *  *
 *
 */

package com.dtstack.flinkx.connector.influxdb.source;

import com.dtstack.flinkx.conf.SyncConf;
import com.dtstack.flinkx.connector.influxdb.conf.InfluxdbSourceConfig;
import com.dtstack.flinkx.connector.influxdb.converter.InfluxdbRowTypeConverter;
import com.dtstack.flinkx.converter.RawTypeConverter;
import com.dtstack.flinkx.source.SourceFactory;
import com.dtstack.flinkx.util.GsonUtil;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.util.Map;

/**
 * Company：www.dtstack.com.
 *
 * @author shitou
 * @date 2022/3/8
 */
public class InfluxdbSourceFactory extends SourceFactory {

    private InfluxdbSourceConfig config;

    public InfluxdbSourceFactory(SyncConf syncConf, StreamExecutionEnvironment env) {
        super(syncConf, env);
        Map<String, Object> parameter = syncConf.getJob().getReader().getParameter();
        Gson gson =
                new GsonBuilder()
                        .create();
        GsonUtil.setTypeAdapter(gson);
        this.config = gson.fromJson(gson.toJson(parameter), InfluxdbSourceConfig.class);
        config.setColumn(syncConf.getReader().getFieldList());
        super.initFlinkxCommonConf(config);
    }

    @Override
    public RawTypeConverter getRawTypeConverter() {
        return InfluxdbRowTypeConverter::apply;
    }

    @Override
    public DataStream<RowData> createSource() {
        InfluxdbInputFormatBuilder builder = new InfluxdbInputFormatBuilder();
        builder.setInfluxdbConfig(config);
        return createInput(builder.finish());
    }
}
