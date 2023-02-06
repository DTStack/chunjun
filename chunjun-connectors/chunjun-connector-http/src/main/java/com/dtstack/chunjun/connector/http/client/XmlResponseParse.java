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

import com.dtstack.chunjun.connector.http.common.HttpRestConfig;
import com.dtstack.chunjun.connector.http.util.JsonPathUtil;
import com.dtstack.chunjun.connector.http.util.XmlUtil;
import com.dtstack.chunjun.constants.ConstantValue;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.util.MapUtil;

import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class XmlResponseParse extends ResponseParse {
    private String responseValue;
    private HttpRequestParam requestParam;
    private Iterator<Map<String, Object>> iterator;

    public XmlResponseParse(HttpRestConfig config, AbstractRowConverter converter) {
        super(config, converter);
        if (CollectionUtils.isEmpty(columns)) {
            throw new RuntimeException("please configure column when decode is csv");
        }
    }

    @Override
    public boolean hasNext() throws IOException {
        return iterator.hasNext();
    }

    @Override
    public ResponseValue next() throws Exception {
        Map<String, Object> next = iterator.next();
        if (StringUtils.isBlank(config.getDataSubject())
                && next.containsKey(columns.get(0).getName())) {
            // rootKey
            String key = columns.get(0).getName();
            HashMap<String, Object> data = new HashMap<>();
            data.put(key, next.get(key));
            return new ResponseValue(converter.toInternal(data), requestParam, responseValue);
        }
        LinkedHashMap<String, Object> data =
                buildResponseByKey(next, columns, ConstantValue.POINT_SYMBOL);
        return new ResponseValue(converter.toInternal(data), requestParam, responseValue);
    }

    @Override
    public void parse(String responseValue, int responseStatus, HttpRequestParam requestParam) {
        this.responseValue = responseValue;
        this.requestParam = requestParam;

        Map<String, Object> xmlData = new HashMap<>();
        XmlUtil.xmlParse(responseValue, xmlData);
        if (StringUtils.isBlank(config.getDataSubject())) {
            this.iterator = Lists.newArrayList(xmlData).iterator();
        } else {
            Object valueByKey =
                    MapUtil.getValueByKey(
                            xmlData,
                            JsonPathUtil.parseJsonPath(config.getDataSubject()),
                            ConstantValue.POINT_SYMBOL);
            if (valueByKey instanceof List) {
                this.iterator = ((List) valueByKey).iterator();
            } else if (valueByKey instanceof Map) {
                Map<String, Object> data = (Map<String, Object>) valueByKey;
                this.iterator = Lists.newArrayList(data).iterator();
            } else {
                throw new RuntimeException(
                        config.getDataSubject() + " in response is not array or map");
            }
        }
    }
}
