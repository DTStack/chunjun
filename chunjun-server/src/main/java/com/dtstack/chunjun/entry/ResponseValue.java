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
package com.dtstack.chunjun.entry;

import lombok.Data;

/**
 * web 返回的value
 *
 * @author xuchao
 * @date 2023-09-19
 */
@Data
public class ResponseValue {

    /** 返回状态码 0正常，1 异常 */
    private int code;
    /** 返回结果 */
    private String data;
    /** 状态为异常情况下返回的错误信息 */
    private String errorMsg;
    /** 请求执行耗时 */
    private long space;
}
