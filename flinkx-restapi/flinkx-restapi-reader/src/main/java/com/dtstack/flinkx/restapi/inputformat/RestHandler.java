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

package com.dtstack.flinkx.restapi.inputformat;


import com.dtstack.flinkx.reader.MetaColumn;
import com.dtstack.flinkx.restapi.common.MetaParam;
import com.dtstack.flinkx.restapi.reader.HttpRestConfig;
import com.dtstack.flinkx.restapi.reader.Strategy;
import org.apache.flink.types.Row;

import java.util.List;
import java.util.Map;

/**
 * rest请求的执行
 *
 * @author dujie
 */
public interface RestHandler {

    /**
     * 根据请求参数以及返回的值选择一个策略
     */
    Strategy chooseStrategy(List<Strategy> strategies, Map<String,Object> responseValue, HttpRestConfig restConfig, HttpRequestParam httpRequestParam);

    /**
     * 根据定义的param header结构，上次请求参数和上次请求结果构建出本次请求的header 以及 param
     */
    HttpRequestParam buildRequestParam(List<MetaParam> metaParams, List<MetaParam> metaHeaders, HttpRequestParam prevRequestParam, Map<String,Object> prevResponseValue, HttpRestConfig restConfig, boolean first);

    /**
     * 根据返回的response 构建出row对象
     */
    ResponseValue buildData(String decode,String responseValue, String fields);

}
