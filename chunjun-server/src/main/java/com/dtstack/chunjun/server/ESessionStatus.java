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
package com.dtstack.chunjun.server;

/**
 * session 当前的状态
 *
 * @author xuchao
 * @date 2023-05-17
 */
public enum ESessionStatus {
    // 未初始化状态,表明还为对接到具体的yarn app上
    UNINIT("UNINIT"),
    HEALTHY("HEALTHY"),
    UNHEALTHY("UNHEALTHY");

    private final String value;

    private ESessionStatus(String value) {
        this.value = value;
    }

    public String getValue() {
        return this.getValue();
    }
}
