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

import org.apache.flink.table.data.RowData;

import lombok.AllArgsConstructor;
import lombok.Data;

@AllArgsConstructor
@Data
public class ResponseValue {

    /** 本次请求状态 -1 不正常，代表出现了异常 0 代表结束任务 strategy 出现了stop 1 代表任务正常 */
    private int status;
    /** 返回值 */
    private RowData data;
    /** 如果是异常数据 这个是异常数据 */
    private String errorMsg;
    /** 请求参数 */
    private HttpRequestParam requestParam;

    /** 原始的返回值 */
    private String originResponseValue;

    public ResponseValue(RowData data, HttpRequestParam requestParam, String originResponseValue) {
        this(1, data, null, requestParam, originResponseValue);
    }

    public boolean isNormal() {
        return status != -1;
    }
}
