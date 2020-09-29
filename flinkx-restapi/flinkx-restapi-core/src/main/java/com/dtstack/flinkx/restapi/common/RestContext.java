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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * RestContext
 *
 * @author by dujie@dtstack.com
 * @Date 2020/9/26
 */
public class RestContext implements lifecycle {

    private int time = 0;
    private ThreadLocal<Set<String>> setThreadLocal = new ThreadLocal<>();
    private Map<ParamType, Map<String, Object>> requestValue;
    private Map<ParamType, Map<String, ParamDefinition>> paramDefinitions;


    public RestContext() {
        this.requestValue = new HashMap<>(32);
        this.paramDefinitions = new HashMap<>(32);
    }

    public Map<ParamType, Map<String, Object>> getPreValue() {
        return requestValue;
    }


    public httprequestApi.Httprequest build() {
        // 根据当前value计算出httprequest
        return null;
    }

    public httprequestApi.Httprequest buildNext() {
        // 计算下一次的请求  但是不会更新preValue值
        return null;
    }

    public void update() {
        //更新当前value
        updateValue();
    }


    public void parseAndInt(Map<String, Map<String, String>> job, ParamType paramType) {
        paramDefinitions.put(paramType, ParamFactory.createDefinition(paramType, job, this).stream().collect(Collectors.toMap(ParamDefinition::getName, Function.identity())));
        init();
    }

    @Override
    public void init() {
        paramDefinitions.entrySet().stream().flatMap(item -> item.getValue().entrySet().stream()).forEach(k -> {
            ParamDefinition value = k.getValue();
            value.init();
        });

        //检查循环依赖
        updateValue();
        nextrecyleInit();
        //检查动态变量是否符合正太表达式
        check();
    }

    public int getTime() {
        return time;
    }

    public int addtime() {
        return time++;
    }

    public int jiantime() {
        return time--;
    }


    protected void updateValue() {
        addtime();
        this.requestValue = calcute();
    }

    public void updateByKey(ParamDefinition paramDefinition) {
        requestValue.computeIfAbsent(paramDefinition.getType(), dd -> new HashMap<>(32))
                .put(paramDefinition.getName(), paramDefinition.getValue());
    }


    private Map<ParamType, Map<String, Object>> calcute() {
        Map<ParamType, Map<String, Object>> tempData = new HashMap<>(32);
        paramDefinitions.forEach((k, v) ->
                v.forEach((k1, v1) -> tempData
                        .computeIfAbsent(k, dd -> new HashMap<>(32))
                        .put(k1, v1.getValue())));
        return tempData;
    }

    public Map<ParamType, Map<String, Object>> nextrecyleInit() {
        addtime();
        Map<ParamType, Map<String, Object>> data = calcute();
        jiantime();
        return data;
    }

    public void check() {

    }

    private Object getValueQuick(ParamType type, String key) {
        return requestValue.get(type).get(key);
    }

    protected Object getValue(ParamType type, String key) {
        if (setThreadLocal.get() == null) {
            setThreadLocal.set(new HashSet<>(paramDefinitions.size() * 8));
        }
        if (setThreadLocal.get().contains(type.name() + "." + key)) {
            throw new RuntimeException("循环依赖");
        }
        setThreadLocal.get().add(type.name() + "." + key);
        Object data = getValueQuick(type, key);
        setThreadLocal.get().remove(type.name() + "." + key);
        return data;
    }
    //循环依赖使用有向图判断


    public Map<ParamType, Map<String, ParamDefinition>> getParamDefinitions() {
        return paramDefinitions;
    }
}
