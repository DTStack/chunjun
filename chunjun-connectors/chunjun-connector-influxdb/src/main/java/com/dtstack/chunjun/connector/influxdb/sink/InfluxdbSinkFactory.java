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

package com.dtstack.chunjun.connector.influxdb.sink;

import com.dtstack.chunjun.config.SyncConfig;
import com.dtstack.chunjun.connector.influxdb.config.InfluxdbSinkConfig;
import com.dtstack.chunjun.connector.influxdb.converter.InfluxdbRawTypeMapper;
import com.dtstack.chunjun.converter.RawTypeMapper;
import com.dtstack.chunjun.sink.SinkFactory;
import com.dtstack.chunjun.sink.WriteMode;
import com.dtstack.chunjun.util.GsonUtil;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.data.RowData;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.util.Map;

public class InfluxdbSinkFactory extends SinkFactory {

    protected InfluxdbSinkConfig influxdbConfig;

    public InfluxdbSinkFactory(SyncConfig syncConfig) {
        super(syncConfig);
        Map<String, Object> parameter = syncConfig.getJob().getWriter().getParameter();
        Gson gson = new GsonBuilder().create();
        GsonUtil.setTypeAdapter(gson);
        this.influxdbConfig = gson.fromJson(gson.toJson(parameter), InfluxdbSinkConfig.class);
        Object writeMode = parameter.get("writeMode");
        influxdbConfig.setWriteMode(
                writeMode == null ? WriteMode.APPEND.name() : writeMode.toString());
        influxdbConfig.setColumn(syncConfig.getWriter().getFieldList());
        super.initCommonConf(influxdbConfig);
    }

    @Override
    public RawTypeMapper getRawTypeMapper() {
        return InfluxdbRawTypeMapper::apply;
    }

    @Override
    public DataStreamSink<RowData> createSink(DataStream<RowData> dataSet) {
        InfluxdbOutputFormatBuilder builder = new InfluxdbOutputFormatBuilder();
        builder.setInfluxdbConfig(influxdbConfig);
        builder.setDatabase(influxdbConfig.getDatabase());
        builder.setMeasurement(influxdbConfig.getMeasurement());
        builder.setEnableBatch(influxdbConfig.isEnableBatch());
        builder.setRowConverter(null, useAbstractBaseColumn);
        return createOutput(dataSet, builder.finish());
    }
}
