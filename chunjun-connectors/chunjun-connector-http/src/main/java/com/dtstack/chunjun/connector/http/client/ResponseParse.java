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

import com.dtstack.chunjun.config.FieldConfig;
import com.dtstack.chunjun.connector.http.common.HttpRestConfig;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.util.GsonUtil;
import com.dtstack.chunjun.util.MapUtil;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public abstract class ResponseParse {
    protected final HttpRestConfig config;
    protected final List<FieldConfig> columns;
    protected final AbstractRowConverter converter;

    public ResponseParse(HttpRestConfig config, AbstractRowConverter converter) {
        this.config = config;
        this.columns = config.getColumn();
        this.converter = converter;
    }

    public abstract boolean hasNext() throws IOException;

    public abstract ResponseValue next() throws Exception;

    public abstract void parse(
            String responseValue, int responseStatus, HttpRequestParam requestParam);

    /**
     * 根据指定的key 构建一个新的response
     *
     * @param map 返回值
     * @param columns 指定字段
     */
    protected LinkedHashMap<String, Object> buildResponseByKey(
            Map<String, Object> map, List<FieldConfig> columns, String nested) {
        LinkedHashMap<String, Object> filedValue = new LinkedHashMap<>(columns.size() << 2);
        for (FieldConfig key : columns) {
            if (null != key.getValue()) {
                filedValue.put(key.getName(), key.getValue());
            } else {
                Object value = MapUtil.getValueByKey(map, key.getName(), nested);
                filedValue.put(key.getName(), value);
            }
        }
        return filedValue;
    }

    protected LinkedHashMap<String, Object> buildResponseByKey(
            Map<String, Object> map, List<FieldConfig> columns, boolean useNullReplaceNotExists) {
        LinkedHashMap<String, Object> filedValue = new LinkedHashMap<>(columns.size() << 2);
        for (FieldConfig key : columns) {
            if (null != key.getValue()) {
                filedValue.put(key.getName(), key.getValue());
            } else {
                if (!map.containsKey(key.getName())) {
                    if (useNullReplaceNotExists) {
                        filedValue.put(key.getName(), null);
                    } else {
                        throw new RuntimeException(
                                "not exists column "
                                        + key.getName()
                                        + " in map:  "
                                        + GsonUtil.GSON.toJson(map));
                    }
                } else {
                    Object o = map.get(key.getName());
                    filedValue.put(key.getName(), o);
                }
            }
        }
        return filedValue;
    }
}
