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
        GSON =
                new GsonBuilder()
                        .serializeNulls()
                        .disableHtmlEscaping()
                        .setPrettyPrinting()
                        .create();

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
                                                // Either List or Map
                                                Object current;
                                                JsonToken peeked = in.peek();
                                            
                                                current = tryBeginNesting(in, peeked);
                                                if (current == null) {
                                                    return readTerminal(in, peeked);
                                                }

                                                Deque<Object> stack = new ArrayDeque<>();

                                                while (true) {
                                                    while (in.hasNext()) {
                                                        String name = null;
                                                        // Name is only used for JSON object members
                                                        if (current instanceof Map) {
                                                            name = in.nextName();
                                                        }

                                                        peeked = in.peek();
                                                        Object value = tryBeginNesting(in, peeked);
                                                        boolean isNesting = value != null;

                                                        if (value == null) {
                                                            value = readTerminal(in, peeked);
                                                        }

                                                        if (current instanceof List) {
                                                            @SuppressWarnings("unchecked")
                                                            List<Object> list = (List<Object>) current;
                                                            list.add(value);
                                                        } else {
                                                            @SuppressWarnings("unchecked")
                                                            Map<String, Object> map = (Map<String, Object>) current;
                                                            map.put(name, value);
                                                        }

                                                        if (isNesting) {
                                                            stack.addLast(current);
                                                            current = value;
                                                        }
                                                    }

                                                    // End current element
                                                    if (current instanceof List) {
                                                        in.endArray();
                                                    } else {
                                                        in.endObject();
                                                    }

                                                    if (stack.isEmpty()) {
                                                        return current;
                                                    } else {
                                                        // Continue with enclosing element
                                                        current = stack.removeLast();
                                                    }
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
