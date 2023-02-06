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

package com.dtstack.chunjun.connector.http.client;

import com.dtstack.chunjun.connector.http.common.HttpRestConfig;
import com.dtstack.chunjun.connector.http.common.MetaParam;

import java.util.List;
import java.util.Map;

/**
 * rest请求的执行
 *
 * @author shifang
 */
public interface RestHandler {

    /**
     * 根据请求参数以及返回的值选择一个策略
     *
     * @param strategies 策略
     * @param responseValue 返回值
     * @param restConfig http配置
     * @param httpRequestParam 本次实际请求参数
     * @param metaParams 原始的所有请求参数
     * @return 返回的策略
     */
    Strategy chooseStrategy(
            List<Strategy> strategies,
            Map<String, Object> responseValue,
            HttpRestConfig restConfig,
            HttpRequestParam httpRequestParam,
            List<MetaParam> metaParams);

    /**
     * 根据定义的param body header，上次请求参数和上次请求结果构建出本次请求参数
     *
     * @param metaParams get请求params参数
     * @param metaBodys body参数
     * @param metaHeaders header参数
     * @param prevRequestParam 上一次请求参数
     * @param prevResponseValue 上一次返回值
     * @param restConfig http配置
     * @param first 是否是第一次
     * @return 当前请求值
     */
    HttpRequestParam buildRequestParam(
            List<MetaParam> metaParams,
            List<MetaParam> metaBodys,
            List<MetaParam> metaHeaders,
            HttpRequestParam prevRequestParam,
            Map<String, Object> prevResponseValue,
            HttpRestConfig restConfig,
            boolean first);
}
