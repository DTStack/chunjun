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

import com.dtstack.chunjun.throwable.ChunJunRuntimeException;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.MapperFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static java.util.stream.Collectors.toMap;

public class JsonUtil {

    public static final ObjectMapper objectMapper =
            new ObjectMapper()
                    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                    .configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, true)
                    .setSerializationInclusion(JsonInclude.Include.NON_NULL);

    public static final TypeReference<Map<String, Object>> MAP_TYPE_REFERENCE =
            new TypeReference<Map<String, Object>>() {};

    /**
     * json反序列化成实体对象
     *
     * @param jsonStr json字符串
     * @param clazz 实体类class
     * @param <T> 泛型
     * @return 实体对象
     */
    public static <T> T toObject(String jsonStr, Class<T> clazz) {
        try {
            return objectMapper.readValue(jsonStr, clazz);
        } catch (IOException e) {
            throw new ChunJunRuntimeException(
                    "error parse [" + jsonStr + "] to [" + clazz.getName() + "]", e);
        }
    }

    /**
     * json反序列化成实体对象
     *
     * @param jsonStr json字符串
     * @param clazz 实体类class
     * @param <T> 泛型
     * @return 实体对象
     */
    public static <T> T toObject(String jsonStr, TypeReference<T> valueTypeRef) {
        try {
            return objectMapper.readValue(jsonStr, valueTypeRef);
        } catch (IOException e) {
            throw new ChunJunRuntimeException(
                    "error parse ["
                            + jsonStr
                            + "] to ["
                            + valueTypeRef.getType().getTypeName()
                            + "]",
                    e);
        }
    }

    /**
     * 实体对象转json字符串
     *
     * @param obj 实体对象
     * @return json字符串
     */
    public static String toJson(Object obj) {
        try {
            return objectMapper.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("error parse [" + obj + "] to json", e);
        }
    }

    /**
     * 实体对象转格式化输出的json字符串(用于日志打印)
     *
     * @param obj 实体对象
     * @return 格式化输出的json字符串
     */
    public static String toPrintJson(Object obj) {
        try {
            Map<String, Object> result =
                    objectMapper.readValue(objectMapper.writeValueAsString(obj), HashMap.class);
            MapUtil.replaceAllElement(
                    result,
                    Lists.newArrayList("pwd", "password", "druid.password", "secretKey"),
                    "******");
            return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(result);
        } catch (Exception e) {
            throw new RuntimeException("error parse [" + obj + "] to json", e);
        }
    }

    /**
     * 实体对象转格式化输出的json字符串(用于日志打印)
     *
     * @param obj 实体对象
     * @return 格式化输出的json字符串
     */
    public static String toFormatJson(Object obj) {
        try {
            Map<String, String> collect =
                    ((Properties) obj)
                            .entrySet().stream()
                                    .collect(
                                            toMap(
                                                    v -> v.getKey().toString(),
                                                    v -> v.getValue().toString()));
            return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(collect);
        } catch (Exception e) {
            throw new RuntimeException("error parse [" + obj + "] to json", e);
        }
    }

    /**
     * 实体对象转byte数组
     *
     * @param obj 实体对象
     * @return byte数组
     */
    public static byte[] toBytes(Object obj) {
        try {
            return objectMapper.writeValueAsBytes(obj);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("error parse [" + obj + "] to json", e);
        }
    }
}
