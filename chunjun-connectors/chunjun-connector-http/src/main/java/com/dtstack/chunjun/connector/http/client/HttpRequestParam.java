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
package com.dtstack.chunjun.connector.http.client;

import com.dtstack.chunjun.connector.http.common.MetaParam;
import com.dtstack.chunjun.util.GsonUtil;
import com.dtstack.chunjun.util.MapUtil;

import java.util.HashMap;
import java.util.Map;

public class HttpRequestParam {

    private final Map<String, Object> body = new HashMap<>(32);
    private final Map<String, Object> header = new HashMap<>(32);
    private final Map<String, Object> param = new HashMap<>(32);

    /**
     * 将动态参数根据按照key是否嵌套，获取真正的key 将值放入对应的map中
     *
     * @param metaParam 动态参数
     * @param fieldDelimiter 切割键
     * @param value 动态参数当前对应的值
     */
    public void putValue(MetaParam metaParam, String fieldDelimiter, Object value) {
        if (null == metaParam.getIsNest()) {
            fieldDelimiter = null;
        } else {
            fieldDelimiter = metaParam.getIsNest() ? fieldDelimiter : null;
        }
        switch (metaParam.getParamType()) {
            case BODY:
                MapUtil.buildMap(metaParam.getKey(), fieldDelimiter, value, getBody());
                break;
            case HEADER:
                MapUtil.buildMap(metaParam.getKey(), fieldDelimiter, value, getHeader());
                break;
            case PARAM:
                MapUtil.buildMap(metaParam.getKey(), fieldDelimiter, value, getParam());
                break;
            default:
                throw new UnsupportedOperationException(
                        "HttpRequestParam not supported " + metaParam.getParamType());
        }
    }

    /**
     * 获取动态参数的值
     *
     * @param metaParam 动态参数
     * @param fieldDelimiter 切割键
     * @return
     */
    public Object getValue(MetaParam metaParam, String fieldDelimiter) {
        if (null == metaParam.getIsNest()) {
            fieldDelimiter = null;
        } else {
            fieldDelimiter = metaParam.getIsNest() ? fieldDelimiter : null;
        }
        switch (metaParam.getParamType()) {
            case BODY:
                return MapUtil.getValueByKey(getBody(), metaParam.getKey(), fieldDelimiter);
            case HEADER:
                return MapUtil.getValueByKey(getHeader(), metaParam.getKey(), fieldDelimiter);
            case PARAM:
                return MapUtil.getValueByKey(getParam(), metaParam.getKey(), fieldDelimiter);
            default:
                throw new UnsupportedOperationException(
                        "HttpRequestParam not supported " + metaParam.getParamType().name());
        }
    }

    /**
     * 是否包含动态参数对应的key
     *
     * @param metaParam 动态参数
     * @param fieldDelimiter 切割键
     * @return
     */
    public boolean containsKey(MetaParam metaParam, String fieldDelimiter) {
        if (null == metaParam.getIsNest()) {
            fieldDelimiter = null;
        } else {
            fieldDelimiter = metaParam.getIsNest() ? fieldDelimiter : null;
        }
        try {
            switch (metaParam.getParamType()) {
                case BODY:
                    MapUtil.getValueByKey(getBody(), metaParam.getKey(), fieldDelimiter);
                    return true;
                case HEADER:
                    MapUtil.getValueByKey(getHeader(), metaParam.getKey(), fieldDelimiter);
                    return true;
                case PARAM:
                    MapUtil.getValueByKey(getParam(), metaParam.getKey(), fieldDelimiter);
                    return true;
                default:
                    throw new UnsupportedOperationException(
                            "HttpRequestParam not supported " + metaParam.getParamType().name());
            }
        } catch (RuntimeException e) {
            if (e instanceof UnsupportedOperationException) {
                throw e;
            } else {
                return false;
            }
        }
    }

    public Map<String, Object> getBody() {
        return body;
    }

    public Map<String, Object> getHeader() {
        return header;
    }

    public Map<String, Object> getParam() {
        return param;
    }

    public static HttpRequestParam copy(HttpRequestParam source) {
        HttpRequestParam requestParam = new HttpRequestParam();
        requestParam.getParam().putAll(source.getParam());
        requestParam.getBody().putAll(source.getBody());
        requestParam.getHeader().putAll(source.getHeader());

        return requestParam;
    }

    @Override
    public String toString() {
        return "HttpRequestParam{"
                + "body="
                + GsonUtil.GSON.toJson(body)
                + ", header="
                + GsonUtil.GSON.toJson(header)
                + ", param="
                + GsonUtil.GSON.toJson(param)
                + '}';
    }
}
