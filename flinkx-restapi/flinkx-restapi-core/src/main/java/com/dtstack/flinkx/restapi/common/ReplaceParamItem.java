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

import java.util.Collections;
import java.util.Locale;
import java.util.Optional;

/**
 * ReplaceParamItem
 *
 * @author by dujie@dtstack.com
 * @Date 2020/9/26
 */
public class ReplaceParamItem implements Paramitem {
    private String key;
    private ParamType paramType;
//    private ParamDefinition paramDefinition;
    private RestContext restContext;

    public ReplaceParamItem(String key,  RestContext restContext) {
        int i = key.indexOf(".");
        this.paramType = ParamType.valueOf(key.substring(0, i).toUpperCase(Locale.ENGLISH));
        this.key = key.substring(i+1);
//        this.paramDefinition = paramDefinition;
        this.restContext = restContext;
    }

    @Override
    public Object getValue() {
        return restContext.getValue(paramType, key);
    }

//    @Override
//    public ParamDefinition getParamDefinition() {
//        return paramDefinition;
//    }

    public ParamDefinition getReplaceParamDefinition(RestContext restContext) {
        return Optional.ofNullable(restContext.getParamDefinitions().get(paramType)).orElse(Collections.emptyMap()).get(key);
    }

}
