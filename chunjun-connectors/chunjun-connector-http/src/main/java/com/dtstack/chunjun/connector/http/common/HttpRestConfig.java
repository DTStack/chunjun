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
package com.dtstack.chunjun.connector.http.common;

import com.dtstack.chunjun.config.CommonConfig;
import com.dtstack.chunjun.connector.http.client.Strategy;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.ArrayList;
import java.util.List;

@EqualsAndHashCode(callSuper = true)
@Data
public class HttpRestConfig extends CommonConfig {

    private static final long serialVersionUID = 1100442976901794740L;

    /** https/http */
    private String protocol = "https";

    /** http address */
    private String url;

    /** post/get */
    private String requestMode;

    private String fieldDelimiter = com.dtstack.chunjun.constants.ConstantValue.POINT_SYMBOL;

    /** response text/json */
    private String decode = "text";

    /** decode为json时，指定解析的key */
    private String fields;

    /** decode为json时，指定解析key的类型 */
    private String fieldTypes;

    /** 请求的间隔时间 单位毫秒 */
    private Long intervalTime;

    /** 请求的header头 */
    private List<MetaParam> header = new ArrayList<>(2);

    /** 请求的param */
    private List<MetaParam> param = new ArrayList<>(2);

    /** 请求的body */
    private List<MetaParam> body = new ArrayList<>(2);

    /** 返回结果的处理策略 */
    protected List<Strategy> strategy = new ArrayList<>(2);

    public boolean isJsonDecode() {
        return getDecode().equalsIgnoreCase(ConstantValue.DEFAULT_DECODE);
    }
}
