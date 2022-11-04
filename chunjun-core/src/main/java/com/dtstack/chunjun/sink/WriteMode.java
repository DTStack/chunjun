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
package com.dtstack.chunjun.sink;

public enum WriteMode {

    /** 用于关系数据库的直接写入与ftp的文件追加写入 */
    INSERT("insert"),

    /** 用于关系数据库的更新操作 */
    UPDATE("update"),

    /** 用于MySQL的替换操作 */
    REPLACE("replace"),

    /** 用于文件的覆盖 */
    OVERWRITE("overwrite"),

    /** 用于文件的追加 */
    APPEND("append"),

    UPSERT("upsert"),

    NONCONFLICT("nonconflict"),
    ;

    private final String mode;

    WriteMode(String mode) {
        this.mode = mode;
    }

    public String getMode() {
        return mode;
    }
}
