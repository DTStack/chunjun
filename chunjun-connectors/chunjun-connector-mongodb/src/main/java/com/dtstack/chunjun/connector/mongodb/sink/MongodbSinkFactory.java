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

package com.dtstack.chunjun.connector.mongodb.sink;

import com.dtstack.chunjun.conf.SyncConf;
import com.dtstack.chunjun.connector.mongodb.converter.MongodbRawTypeConverter;
import com.dtstack.chunjun.connector.mongodb.datasync.MongoConverterFactory;
import com.dtstack.chunjun.connector.mongodb.datasync.MongodbDataSyncConf;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.converter.RawTypeConverter;
import com.dtstack.chunjun.sink.SinkFactory;
import com.dtstack.chunjun.util.GsonUtil;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.data.RowData;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * @author Ada Wong
 * @program flinkx
 * @create 2021/06/21
 */
public class MongodbSinkFactory extends SinkFactory {

    private final MongodbDataSyncConf mongodbDataSyncConf;

    public MongodbSinkFactory(SyncConf syncConf) {
        super(syncConf);
        Gson gson = new GsonBuilder().create();
        GsonUtil.setTypeAdapter(gson);
        mongodbDataSyncConf =
                gson.fromJson(
                        gson.toJson(syncConf.getWriter().getParameter()),
                        MongodbDataSyncConf.class);
    }

    @Override
    public RawTypeConverter getRawTypeConverter() {
        return MongodbRawTypeConverter::apply;
    }

    @Override
    public DataStreamSink<RowData> createSink(DataStream<RowData> dataSet) {
        MongodbOutputFormatBuilder builder = new MongodbOutputFormatBuilder(mongodbDataSyncConf);
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
        return createOutput(dataSet, builder.finish());
    }
}
