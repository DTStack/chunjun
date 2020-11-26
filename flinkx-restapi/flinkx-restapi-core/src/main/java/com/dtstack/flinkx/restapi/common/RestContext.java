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
package com.dtstack.flinkx.restapi.common;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * RestContext
 *
 * @author by dujie@dtstack.com
 * @Date 2020/9/26
 */
public class RestContext {

    private  String requestType;

    private String url;

    private String format;

    private boolean first;

    private ThreadLocal<Set<String>> setThreadLocal = new ThreadLocal<>();

    private Map<String, Object> prevRequestValue = new HashMap<>(16);

    private Map<String, Object> requestValue;

    private Map<ParamType, Map<String, ParamDefinition>> paramDefinitions;

    private Object object = new Object();

    public RestContext(String requestType,String url,String format) {
        this.requestType =requestType;
        this.url =url;
        this.requestValue = new HashMap<>(32);
        this.paramDefinitions = new HashMap<>(32);
        this.first = true;
        this.format=format;
    }

    public void updateValue(){
        prevRequestValue = requestValue;
        requestValue=new HashMap<>(requestValue.size());
    }

    public Map<String, Object> getPreValue() {
        return requestValue;
    }


    public httprequestApi.Httprequest build() {
        // 根据当前value计算出httprequest
        httprequestApi.Httprequest httprequest = new httprequestApi.Httprequest();

        Map<String, Object> body = new HashMap<>(16);
        Map<String, String> header = new HashMap<>(16);
        Map<String, Object> param = new HashMap<>(16);
        if (first) {
            paramDefinitions.get(ParamType.BODY).forEach((k, v) -> {
                body.put(k, v.getValue());
            });

            paramDefinitions.get(ParamType.HEADER).forEach((k, v) -> {
                header.put(k, v.getValue().toString());
            });

            paramDefinitions.get(ParamType.PARAM).forEach((k, v) -> {
                param.put(k, v.getValue());
            });
            first = false;
        } else {
            paramDefinitions.get(ParamType.BODY).forEach((k, v) -> {
                if (v instanceof ParamDefinitionNextAble) {
                    body.put(k, ((ParamDefinitionNextAble) v).getNextValue());
                } else {
                    body.put(k, v.getValue());
                }
            });

            paramDefinitions.get(ParamType.HEADER).forEach((k, v) -> {
                if (v instanceof ParamDefinitionNextAble) {
                    header.put(k, ((ParamDefinitionNextAble) v).getNextValue().toString());
                } else {
                    header.put(k, v.getValue().toString());
                }
            });

            paramDefinitions.get(ParamType.PARAM).forEach((k, v) -> {
                if (v instanceof ParamDefinitionNextAble) {
                    param.put(k, ((ParamDefinitionNextAble) v).getNextValue());
                } else {
                    param.put(k, v.getValue());
                }
            });
        }
        httprequest.buildBody(body).buildHeader(header).buildParam(param);
        return httprequest;
    }

    public httprequestApi.Httprequest buildNext() {
        // 计算下一次的请求  但是不会更新preValue值
        return null;
    }

    public void parseAndInt(Map<String, Map<String, String>> job, ParamType paramType) {
        paramDefinitions.put(paramType, ParamFactory.createDefinition(paramType, job, this).stream().collect(Collectors.toMap(ParamDefinition::getName, Function.identity())));
    }



    protected void updateValue(String key, Object value) {
        requestValue.put(key, value);
    }

    private Object getValueQuick(String key) {
        return prevRequestValue.get(key);
    }

    protected Object getValue(String key) {
        if (setThreadLocal.get() == null) {
            setThreadLocal.set(new HashSet<>(paramDefinitions.size() * 8));
        }
        if (setThreadLocal.get().contains(key)) {
            throw new RuntimeException("循环依赖");
        }
        setThreadLocal.get().add(key);
        Object data = getValueQuick(key);
        if (Objects.isNull(data) && first) {
            String[] s = key.split("\\.");
            ParamType paramType = ParamType.valueOf(s[0].toUpperCase(Locale.ENGLISH));
            if (Objects.isNull(paramDefinitions.get(paramType))) {
                data = null;
            } else {
                ParamDefinition  definition = paramDefinitions.get(paramType).get(s[1]);
                //没查到对应的动态变量
                if (Objects.isNull(definition)) {
                    prevRequestValue.put(key, object);
                } else {
                    Object value = definition.getValue();
                    data=value;
                    prevRequestValue.put(key,value );
                }
            }
        }
        setThreadLocal.get().remove(key);

        if (data == object) {
            data = null;
        }
        return data;
    }
    //循环依赖使用有向图判断

    public Map<ParamType, Map<String, ParamDefinition>> getParamDefinitions() {
        return paramDefinitions;
    }

    public String getRequestType() {
        return requestType;
    }

    public String getUrl() {
        return url;
    }

    public String getFormat() {
        return format;
    }

}
