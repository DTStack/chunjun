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

package com.dtstack.chunjun.connector.redis.sink;

import com.dtstack.chunjun.conf.SyncConf;
import com.dtstack.chunjun.connector.redis.adapter.RedisDataModeAdapter;
import com.dtstack.chunjun.connector.redis.adapter.RedisDataTypeAdapter;
import com.dtstack.chunjun.connector.redis.conf.RedisConf;
import com.dtstack.chunjun.connector.redis.converter.RedisColumnConverter;
import com.dtstack.chunjun.connector.redis.enums.RedisDataMode;
import com.dtstack.chunjun.connector.redis.enums.RedisDataType;
import com.dtstack.chunjun.converter.RawTypeConverter;
import com.dtstack.chunjun.sink.SinkFactory;
import com.dtstack.chunjun.util.GsonUtil;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.data.RowData;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * @author chuixue
 * @create 2021-06-16 15:14
 * @description
 */
public class RedisSinkFactory extends SinkFactory {

    private final RedisConf redisConf;

    public RedisSinkFactory(SyncConf syncConf) {
        super(syncConf);
        Gson gson =
                new GsonBuilder()
                        .registerTypeAdapter(RedisDataMode.class, new RedisDataModeAdapter())
                        .registerTypeAdapter(RedisDataType.class, new RedisDataTypeAdapter())
                        .create();
        GsonUtil.setTypeAdapter(gson);
        redisConf =
                gson.fromJson(gson.toJson(syncConf.getWriter().getParameter()), RedisConf.class);
        redisConf.setColumn(syncConf.getWriter().getFieldList());
        super.initFlinkxCommonConf(redisConf);
    }

    @Override
    public RawTypeConverter getRawTypeConverter() {
        return null;
    }

    @Override
    public DataStreamSink<RowData> createSink(DataStream<RowData> dataSet) {
        if (!useAbstractBaseColumn) {
            throw new UnsupportedOperationException("redis not support transform");
        }
        RedisOutputFormatBuilder builder = new RedisOutputFormatBuilder();
        builder.setRedisConf(redisConf);
        builder.setRowConverter(new RedisColumnConverter(redisConf));
        return createOutput(dataSet, builder.finish());
    }
}
