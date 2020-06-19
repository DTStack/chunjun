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
package com.dtstack.flinkx.util;

import com.google.gson.*;
import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Type;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Date: 2020/06/12
 * Company: www.dtstack.com
 *
 *  Gson工具类，用于对json的序列化及反序列化，及解决int类型在map中被转换成double类型问题
 *
 * @author tudou
 */
public class GsonUtil {
    public static Gson GSON = new GsonBuilder()
            .registerTypeAdapter(
                    new TypeToken<TreeMap<String, Object>>(){}.getType(),
                    (JsonDeserializer<TreeMap<String, Object>>) (json, typeOfT, context) -> {

                        TreeMap<String, Object> treeMap = new TreeMap<>();
                        JsonObject jsonObject = json.getAsJsonObject();
                        Set<Map.Entry<String, JsonElement>> entrySet = jsonObject.entrySet();
                        for (Map.Entry<String, JsonElement> entry : entrySet) {
                            Object ot = entry.getValue();
                            if(ot instanceof JsonPrimitive){
                                treeMap.put(entry.getKey(), ((JsonPrimitive) ot).getAsString());
                            }else{
                                treeMap.put(entry.getKey(), ot);
                            }
                        }
                        return treeMap;
                    }).create();

    public static Type gsonMapTypeToken = new TypeToken<TreeMap<String, Object>>(){}.getType();
}
