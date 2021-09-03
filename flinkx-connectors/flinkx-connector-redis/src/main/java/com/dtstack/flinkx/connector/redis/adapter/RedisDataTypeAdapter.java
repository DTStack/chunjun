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

package com.dtstack.flinkx.connector.redis.adapter;

import com.dtstack.flinkx.connector.redis.enums.RedisDataType;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import java.lang.reflect.Type;

/**
 * @author chuixue
 * @create 2021-06-17 17:14
 * @description
 */
public class RedisDataTypeAdapter
        implements JsonSerializer<RedisDataType>, JsonDeserializer<RedisDataType> {

    @Override
    public JsonElement serialize(
            RedisDataType src, Type typeOfSrc, JsonSerializationContext context) {
        return new JsonPrimitive(src.type);
    }

    @Override
    public RedisDataType deserialize(
            JsonElement json, Type typeOfT, JsonDeserializationContext context)
            throws JsonParseException {
        return RedisDataType.getDataType(json.getAsString());
    }
}
