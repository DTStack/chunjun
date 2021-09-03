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
package com.dtstack.flinkx.enums;

/**
 * Date: 2021/04/27 Company: www.dtstack.com
 *
 * @author tudou
 */
public enum OperationType {
    // ----------------- DML -----------------
    INSERT(0, "INSERT"),
    UPDATE(1, "UPDATE"),
    UPDATE_BEFORE(3, "UPDATE"),
    UPDATE_AFTER(4, "UPDATE"),
    DELETE(5, "DELETE"),
    // ----------------- DDL -----------------
    CREATE(10, "CREATE"),
    DROP(11, "DROP"),
    ALTER(12, "ALTER");

    private final int type;
    private final String name;

    OperationType(int type, String name) {
        this.type = type;
        this.name = name;
    }

    public int getType() {
        return type;
    }

    public String getName() {
        return name;
    }
}
