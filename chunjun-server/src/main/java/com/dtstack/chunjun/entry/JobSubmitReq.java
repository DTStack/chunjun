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
 * 提交任务运行的请求
 *
 * @author xuchao
 * @date 2023-09-21
 */
@Data
public class JobSubmitReq {

    private String jobName;

    /** 提交的任务信息-json 格式 */
    private String job;

    /** 任务额外属性信息 */
    private String confProp;

    private boolean isURLDecode = false;
}
