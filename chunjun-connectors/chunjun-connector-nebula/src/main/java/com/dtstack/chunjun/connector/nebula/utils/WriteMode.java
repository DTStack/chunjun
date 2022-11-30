package com.dtstack.chunjun.connector.nebula.utils;
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

/**
 * @author: gaoasi
 * @email: aschaser@163.com
 * @date: 2022/11/14 2:29 下午
 */
public enum WriteMode {

    /** INSERT write mode */
    INSERT("insert"),

    /** UPDATE write mode */
    UPDATE("update"),

    /** DELETE write mode */
    DELETE("delete"),

    /** upsert write mode */
    UPSERT("upsert");

    private String mode;

    public String getMode() {
        return mode;
    }

    public void setMode(String mode) {
        this.mode = mode;
    }

    WriteMode(String mode) {
        this.mode = mode;
    }
}
