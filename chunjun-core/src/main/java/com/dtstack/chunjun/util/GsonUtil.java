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
package com.dtstack.chunjun.util;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.TypeAdapter;
import com.google.gson.TypeAdapterFactory;
import com.google.gson.internal.LinkedTreeMap;
import com.google.gson.internal.bind.ObjectTypeAdapter;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class GsonUtil {
    public static Gson GSON = getGson();
    public static Type gsonMapTypeToken = new TypeToken<HashMap<String, Object>>() {}.getType();

    private static Gson getGson() {
        GSON = new GsonBuilder().disableHtmlEscaping().setPrettyPrinting().create();

        return setTypeAdapter(GSON);
    }

    @SuppressWarnings("all")
    public static Gson setTypeAdapter(Gson gson) {
        try {
            Field factories = Gson.class.getDeclaredField("factories");
            factories.setAccessible(true);
            Object o = factories.get(gson);
            Class<?>[] declaredClasses = Collections.class.getDeclaredClasses();
            for (Class c : declaredClasses) {
                if ("java.util.Collections$UnmodifiableList".equals(c.getName())) {
                    Field listField = c.getDeclaredField("list");
                    listField.setAccessible(true);
                    List<TypeAdapterFactory> list = (List<TypeAdapterFactory>) listField.get(o);
                    int i = list.indexOf(ObjectTypeAdapter.FACTORY);
                    list.set(
                            i,
                            new TypeAdapterFactory() {
                                @Override
                                public <T> TypeAdapter<T> create(Gson gson, TypeToken<T> type) {
                                    if (type.getRawType() == Object.class) {
                                        return new TypeAdapter() {
                                            @Override
                                            public Object read(JsonReader in) throws IOException {
                                                JsonToken token = in.peek();
                                                // 判断字符串的实际类型
                                                switch (token) {
                                                    case BEGIN_ARRAY:
                                                        List<Object> list = new ArrayList<>();
                                                        in.beginArray();
                                                        while (in.hasNext()) {
                                                            list.add(read(in));
                                                        }
                                                        in.endArray();
                                                        return list;

                                                    case BEGIN_OBJECT:
                                                        Map<String, Object> map =
                                                                new LinkedTreeMap<>();
                                                        in.beginObject();
                                                        while (in.hasNext()) {
                                                            map.put(in.nextName(), read(in));
                                                        }
                                                        in.endObject();
                                                        return map;
                                                    case STRING:
                                                        return in.nextString();
                                                    case NUMBER:
                                                        String s = in.nextString();
                                                        if (s.contains(".")) {
                                                            return Double.valueOf(s);
                                                        } else {
                                                            try {
                                                                return Integer.valueOf(s);
                                                            } catch (Exception e) {
                                                                try {
                                                                    return Long.valueOf(s);
                                                                } catch (Exception e1) {
                                                                    return new BigInteger(s);
                                                                }
                                                            }
                                                        }
                                                    case BOOLEAN:
                                                        return in.nextBoolean();
                                                    case NULL:
                                                        in.nextNull();
                                                        return null;
                                                    default:
                                                        throw new IllegalStateException();
                                                }
                                            }

                                            @Override
                                            public void write(JsonWriter out, Object value)
                                                    throws IOException {
                                                if (value == null) {
                                                    out.nullValue();
                                                    return;
                                                }
                                                //noinspection unchecked
                                                TypeAdapter<Object> typeAdapter =
                                                        gson.getAdapter(
                                                                (Class<Object>) value.getClass());
                                                if (typeAdapter instanceof ObjectTypeAdapter) {
                                                    out.beginObject();
                                                    out.endObject();
                                                    return;
                                                }
                                                typeAdapter.write(out, value);
                                            }
                                        };
                                    }
                                    return null;
                                }
                            });
                    break;
                }
            }
        } catch (Exception e) {
            log.error(ExceptionUtil.getErrorMessage(e));
        }
        return gson;
    }
}
