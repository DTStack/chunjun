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
import com.dtstack.chunjun.connector.http.util.JsonPathUtil;
import com.dtstack.chunjun.constants.ConstantValue;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.util.GsonUtil;
import com.dtstack.chunjun.util.MapUtil;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class JsonResponseParse extends ResponseParse {
    private String responseValue;
    private HttpRequestParam requestParam;
    private final Gson gson;
    private final List<FieldConfig> fields;
    private Iterator<Map<String, Object>> iterator;

    public JsonResponseParse(HttpRestConfig config, AbstractRowConverter converter) {
        super(config, converter);
        this.gson = GsonUtil.setTypeAdapter(new Gson());
        if (StringUtils.isNotBlank(config.getFields())) {
            fields =
                    Arrays.stream(config.getFields().split(","))
                            .map(
                                    i -> {
                                        FieldConfig fieldConfig = new FieldConfig();
                                        fieldConfig.setName(i);
                                        return fieldConfig;
                                    })
                            .collect(Collectors.toList());
        } else {
            fields = null;
        }
    }

    @Override
    public boolean hasNext() throws IOException {
        return iterator.hasNext();
    }

    @Override
    public ResponseValue next() throws Exception {
        Map next = iterator.next();

        if (CollectionUtils.isEmpty(columns)) {
            if (CollectionUtils.isNotEmpty(fields)) {
                LinkedHashMap<String, Object> map =
                        buildResponseByKey(next, fields, ConstantValue.POINT_SYMBOL);
                HashMap<String, Object> data = new HashMap<>();
                // 需要拆分key
                ((Map<String, Object>) map)
                        .forEach(
                                (k, v) -> {
                                    MapUtil.buildMap(k, ConstantValue.POINT_SYMBOL, v, data);
                                });
                return new ResponseValue(converter.toInternal(data), requestParam, responseValue);
            } else {
                return new ResponseValue(converter.toInternal(next), requestParam, responseValue);
            }

        } else {
            LinkedHashMap<String, Object> data =
                    buildResponseByKey(next, columns, ConstantValue.POINT_SYMBOL);
            return new ResponseValue(converter.toInternal(data), requestParam, responseValue);
        }
    }

    @Override
    public void parse(String responseValue, int responseStatus, HttpRequestParam requestParam) {
        this.responseValue = responseValue;
        this.requestParam = requestParam;

        Map<String, Object> map = gson.fromJson(responseValue, GsonUtil.gsonMapTypeToken);
        if (StringUtils.isNotBlank(config.getDataSubject())) {
            Object valueByKey =
                    MapUtil.getValueByKey(
                            map,
                            JsonPathUtil.parseJsonPath(config.getDataSubject()),
                            ConstantValue.POINT_SYMBOL);
            if (valueByKey instanceof List) {
                this.iterator = ((List) valueByKey).iterator();
            } else {
                throw new RuntimeException(config.getDataSubject() + " in response is not array");
            }
        } else {
            this.iterator = Lists.newArrayList(map).iterator();
        }
    }
}
