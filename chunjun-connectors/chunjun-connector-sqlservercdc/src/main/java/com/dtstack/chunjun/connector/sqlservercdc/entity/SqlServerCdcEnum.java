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
package com.dtstack.chunjun.connector.sqlservercdc.entity;

import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public enum SqlServerCdcEnum {

    /** 操作未知 */
    UNKNOWN(-1, "unknown"),
    /** 删除操作 */
    DELETE(1, "delete"),
    /** 插入操作 */
    INSERT(2, "insert"),
    /** 更新前操作 */
    UPDATE_BEFORE(3, "update_before"),
    /** 更新后操作 */
    UPDATE_AFTER(4, "update_after"),

    UPDATE(5, "update");

    public final int code;
    public final String name;

    SqlServerCdcEnum(int code, String name) {
        this.code = code;
        this.name = name;
    }

    public static SqlServerCdcEnum getEnum(String name) {
        switch (name.toLowerCase()) {
            case "delete":
                return DELETE;
            case "insert":
                return INSERT;
            case "update_before":
                return UPDATE_BEFORE;
            case "update_after":
                return UPDATE_AFTER;
            default:
                return UNKNOWN;
        }
    }

    public static SqlServerCdcEnum getEnum(int code) {
        switch (code) {
            case 1:
                return DELETE;
            case 2:
                return INSERT;
            case 3:
                return UPDATE_BEFORE;
            case 4:
                return UPDATE_AFTER;
            default:
                return UNKNOWN;
        }
    }

    public static Set<Integer> transform(String name) {
        if (Objects.equals(name, UPDATE.name)) {
            Set<Integer> set = new HashSet<>();
            set.add(UPDATE_BEFORE.code);
            set.add(UPDATE_AFTER.code);
            return set;
        } else {
            return Collections.singleton(getEnum(name).code);
        }
    }
}
