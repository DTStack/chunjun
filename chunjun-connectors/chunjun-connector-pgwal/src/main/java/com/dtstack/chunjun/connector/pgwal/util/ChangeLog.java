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
package com.dtstack.chunjun.connector.pgwal.util;

import java.util.List;

/** */
public class ChangeLog {
    private long transactionId;
    private String id;
    private String schema;
    private String table;
    private List<ColumnInfo> columnList;
    private Object[] oldData;
    private Object[] newData;
    private PgMessageTypeEnum type;

    private long currentLsn;
    private long ts;

    public ChangeLog(String schema, String table, List<ColumnInfo> columnList) {
        this.schema = schema;
        this.table = table;
        this.columnList = columnList;
        this.id = schema + "." + table;
    }

    public ChangeLog() {}

    public ChangeLog(PgMessageTypeEnum type, long transactionId, long ts) {
        this.type = type;
        this.ts = ts;
        this.transactionId = transactionId;
    }

    public static ChangeLog init() {
        return new ChangeLog();
    }

    public String getId() {
        return id;
    }

    public String getSchema() {
        return schema;
    }

    public String getTable() {
        return table;
    }

    public List<ColumnInfo> getColumnList() {
        return columnList;
    }

    public Object[] getOldData() {
        return oldData;
    }

    public void setOldData(Object[] oldData) {
        this.oldData = oldData;
    }

    public Object[] getNewData() {
        return newData;
    }

    public void setNewData(Object[] newData) {
        this.newData = newData;
    }

    public PgMessageTypeEnum getType() {
        return type;
    }

    public void setType(PgMessageTypeEnum type) {
        this.type = type;
    }

    public long getCurrentLsn() {
        return currentLsn;
    }

    public void setCurrentLsn(long currentLsn) {
        this.currentLsn = currentLsn;
    }

    public long getTs() {
        return ts;
    }

    public void setTs(long ts) {
        this.ts = ts;
    }
}
