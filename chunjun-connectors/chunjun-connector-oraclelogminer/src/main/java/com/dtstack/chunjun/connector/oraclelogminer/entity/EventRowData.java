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

package com.dtstack.chunjun.connector.oraclelogminer.entity;

import java.util.StringJoiner;

public class EventRowData {
    /** fieldName * */
    private String name;
    /** field value * */
    private String data;

    private boolean isNull;

    public EventRowData(String name, String data, boolean isNull) {
        this.name = name;
        this.data = data;
        this.isNull = isNull;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }

    public boolean isNull() {
        return isNull;
    }

    public void setNull(boolean aNull) {
        isNull = aNull;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", EventRowData.class.getSimpleName() + "[", "]")
                .add("name='" + name + "'")
                .add("data='" + data + "'")
                .add("isNull=" + isNull)
                .toString();
    }
}
