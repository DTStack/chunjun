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

package com.dtstack.chunjun.connector.redis.source;

import com.dtstack.chunjun.conf.SyncConf;
import com.dtstack.chunjun.connector.redis.adapter.RedisDataModeAdapter;
import com.dtstack.chunjun.connector.redis.adapter.RedisDataTypeAdapter;
import com.dtstack.chunjun.connector.redis.conf.RedisConf;
import com.dtstack.chunjun.connector.redis.converter.RedisColumnConverter;
import com.dtstack.chunjun.connector.redis.converter.RedisRawTypeConverter;
import com.dtstack.chunjun.connector.redis.enums.RedisDataMode;
import com.dtstack.chunjun.connector.redis.enums.RedisDataType;
import com.dtstack.chunjun.converter.RawTypeConverter;
import com.dtstack.chunjun.source.SourceFactory;
import com.dtstack.chunjun.util.GsonUtil;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/** @Author OT @Date 2022/7/27 */
public class RedisSourceFactory extends SourceFactory {
    private RedisConf redisConf;

    public RedisSourceFactory(SyncConf syncConf, StreamExecutionEnvironment env) {
        super(syncConf, env);
        Gson gson =
                new GsonBuilder()
                        .registerTypeAdapter(RedisDataMode.class, new RedisDataModeAdapter())
                        .registerTypeAdapter(RedisDataType.class, new RedisDataTypeAdapter())
                        .create();
        GsonUtil.setTypeAdapter(gson);
        redisConf =
                gson.fromJson(gson.toJson(syncConf.getReader().getParameter()), RedisConf.class);
        redisConf.setColumn(syncConf.getReader().getFieldList());
        super.initCommonConf(redisConf);
    }

    @Override
    public RawTypeConverter getRawTypeConverter() {
        return RedisRawTypeConverter::apply;
    }

    @Override
    public DataStream<RowData> createSource() {
        if (!useAbstractBaseColumn) {
            throw new UnsupportedOperationException("redis not support transform");
        }
        RedisInputFormatBuilder builder = new RedisInputFormatBuilder();
        builder.setRedisConf(redisConf);
        builder.setRowConverter(new RedisColumnConverter(redisConf));
        return createInput(builder.finish());
    }
}
