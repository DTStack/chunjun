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
package com.dtstack.flinkx.restapi.client;

import com.dtstack.flinkx.restapi.common.ParamType;
import com.dtstack.flinkx.util.GsonUtil;

import java.util.HashMap;
import java.util.Map;

/**
 * HttpRequestParam 一次http请求的所有参数
 *
 * @author by dujie@dtstack.com
 * @Date 2020/9/25
 */
public class HttpRequestParam {

    private Map<String, String> body = new HashMap<>(32);
    private Map<String, String> header = new HashMap<>(32);
    private Map<String, String> param = new HashMap<>(32);


    public void putValue(ParamType type, String name, String value) {
        switch (type) {
            case BODY:
                body.put(name, value);
                break;
            case HEADER:
                header.put(name, value);
                break;
            case PARAM:
                param.put(name, value);
                break;
            default:
                throw new UnsupportedOperationException("HttpRequestParam not supported " + type + " to put value, name is " + name + " value is " + value);
        }
    }


    public String getValue(ParamType type, String key) {
        switch (type) {
            case BODY:
                return body.get(key);
            case HEADER:
                return header.get(key);
            case PARAM:
                return param.get(key);
            default:
                throw new UnsupportedOperationException("HttpRequestParam not supported " + type + " to get value, key is " + key);
        }
    }

    public boolean containsParam(ParamType type, String key) {

        switch (type) {
            case BODY:
                return body.containsKey(key);
            case HEADER:
                return header.containsKey(key);
            case PARAM:
                return param.containsKey(key);
            default:
                throw new UnsupportedOperationException("HttpRequestParam not supported  to judge contains key when type is " + type.name() + " ,key is  " + key);
        }

    }

    public Map<String, String> getBody() {
        return body;
    }

    public Map<String, String> getHeader() {
        return header;
    }

    public Map<String, String> getParam() {
        return param;
    }

    public static HttpRequestParam copy(HttpRequestParam source) {
        HttpRequestParam requestParam = new HttpRequestParam();
        source.getBody().forEach((k, v) ->
                requestParam.putValue(ParamType.BODY, k, v));


        source.getParam().forEach((k, v) ->
                requestParam.putValue(ParamType.PARAM, k, v));

        source.getHeader().forEach((k, v) ->
                requestParam.putValue(ParamType.HEADER, k, v));
        return requestParam;
    }

    @Override
    public String toString() {
        return "HttpRequestParam{" +
                "body=" + GsonUtil.GSON.toJson(body) +
                ", header=" + GsonUtil.GSON.toJson(header) +
                ", param=" + GsonUtil.GSON.toJson(param) +
                '}';
    }
}
