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

package com.dtstack.flinkx.cdc.monitor.fetch;

public class Event {
    private final String schema;
    private final String table;
    private final String lsn;
    private final String sql;
    private final Integer status;

    public Event(String schema, String table, String lsn, String sql, Integer status) {
        this.schema = schema;
        this.table = table;
        this.lsn = lsn;
        this.sql = sql;
        this.status = status;
    }

    public String getSchema() {
        return schema;
    }

    public String getTable() {
        return table;
    }

    public String getSql() {
        return sql;
    }

    public Integer getStatus() {
        return status;
    }

    public String getLsn() {
        return lsn;
    }
}
