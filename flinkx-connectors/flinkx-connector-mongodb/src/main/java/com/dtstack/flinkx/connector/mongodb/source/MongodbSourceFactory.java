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

package com.dtstack.flinkx.connector.mongodb.source;

import com.dtstack.flinkx.conf.SyncConf;
import com.dtstack.flinkx.connector.mongodb.converter.MongodbRawTypeConverter;
import com.dtstack.flinkx.connector.mongodb.datasync.MongoConverterFactory;
import com.dtstack.flinkx.connector.mongodb.datasync.MongodbDataSyncConf;
import com.dtstack.flinkx.converter.AbstractRowConverter;
import com.dtstack.flinkx.converter.RawTypeConverter;
import com.dtstack.flinkx.source.SourceFactory;
import com.dtstack.flinkx.util.GsonUtil;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * @author Ada Wong
 * @program flinkx
 * @create 2021/06/21
 */
public class MongodbSourceFactory extends SourceFactory {

    private final MongodbDataSyncConf mongodbDataSyncConf;

    public MongodbSourceFactory(SyncConf syncConf, StreamExecutionEnvironment env) {
        super(syncConf, env);
        Gson gson = new GsonBuilder().create();
        GsonUtil.setTypeAdapter(gson);
        mongodbDataSyncConf =
                gson.fromJson(
                        gson.toJson(syncConf.getReader().getParameter()),
                        MongodbDataSyncConf.class);
    }

    @Override
    public RawTypeConverter getRawTypeConverter() {
        return MongodbRawTypeConverter::apply;
    }

    @Override
    public DataStream<RowData> createSource() {
        MongodbInputFormatBuilder builder = new MongodbInputFormatBuilder(mongodbDataSyncConf);
        MongoConverterFactory mongoConverterFactory =
                new MongoConverterFactory(mongodbDataSyncConf);
        AbstractRowConverter converter;
        if (useAbstractBaseColumn) {
            converter = mongoConverterFactory.createColumnConverter();
        } else {
            converter = mongoConverterFactory.createRowConverter();
        }
        builder.setRowConverter(converter);
        builder.setConfig(mongodbDataSyncConf);
        return createInput(builder.finish());
    }
}
