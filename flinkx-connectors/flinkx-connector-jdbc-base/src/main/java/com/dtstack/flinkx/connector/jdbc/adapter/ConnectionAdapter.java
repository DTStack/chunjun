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
package com.dtstack.flinkx.connector.jdbc.adapter;

import com.dtstack.flinkx.connector.jdbc.conf.ConnectionConf;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import java.lang.reflect.Type;

/**
 * Date: 2021/02/18 Company: www.dtstack.com
 *
 * @author tudou
 */
public class ConnectionAdapter
        implements JsonSerializer<ConnectionConf>, JsonDeserializer<ConnectionConf> {

    /** ReaderConnection or WriterConnection */
    private String className;

    public ConnectionAdapter(String className) {
        this.className = className;
    }

    @Override
    public ConnectionConf deserialize(
            JsonElement json, Type typeOfT, JsonDeserializationContext context)
            throws JsonParseException {
        // 指定包名+类名
        String thePackage = "com.dtstack.flinkx.connector.jdbc.conf." + className;
        try {
            return context.deserialize(json, Class.forName(thePackage));
        } catch (ClassNotFoundException e) {
            throw new JsonParseException("Unknown element type: " + thePackage, e);
        }
    }

    @Override
    public JsonElement serialize(
            ConnectionConf src, Type typeOfSrc, JsonSerializationContext context) {
        JsonObject result = new JsonObject();
        result.add("type", new JsonPrimitive(src.getClass().getSimpleName()));
        result.add("properties", context.serialize(src, src.getClass()));

        return result;
    }
}
