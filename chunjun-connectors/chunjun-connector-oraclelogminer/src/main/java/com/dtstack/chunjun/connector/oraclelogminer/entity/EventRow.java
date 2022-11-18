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

import java.io.Serializable;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.List;
import java.util.StringJoiner;

public class EventRow implements Serializable {
    private static final long serialVersionUID = 1L;

    private List<EventRowData> beforeColumnList;

    private List<EventRowData> afterColumnList;

    private BigInteger scn;

    /** INSERT UPDATE DELETE * */
    private String type;

    private String schema;

    private String table;

    private Long ts;

    private Timestamp opTime;

    public EventRow(
            List<EventRowData> beforeColumn,
            List<EventRowData> afterColumn,
            BigInteger scn,
            String type,
            String schema,
            String table,
            Long ts,
            Timestamp timestamp) {
        this.beforeColumnList = beforeColumn;
        this.afterColumnList = afterColumn;
        this.scn = scn;
        this.type = type;
        this.schema = schema;
        this.table = table;
        this.ts = ts;
        this.opTime = timestamp;
    }

    public List<EventRowData> getBeforeColumnList() {
        return beforeColumnList;
    }

    public void setBeforeColumnList(List<EventRowData> beforeColumnList) {
        this.beforeColumnList = beforeColumnList;
    }

    public List<EventRowData> getAfterColumnList() {
        return afterColumnList;
    }

    public void setAfterColumnList(List<EventRowData> afterColumnList) {
        this.afterColumnList = afterColumnList;
    }

    public BigInteger getScn() {
        return scn;
    }

    public void setScn(BigInteger scn) {
        this.scn = scn;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public Long getTs() {
        return ts;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }

    public Timestamp getOpTime() {
        return opTime;
    }

    public void setOpTime(Timestamp opTime) {
        this.opTime = opTime;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", EventRow.class.getSimpleName() + "[", "]")
                .add("beforeColumnList=" + beforeColumnList)
                .add("afterColumnList=" + afterColumnList)
                .add("scn=" + scn)
                .add("type='" + type + "'")
                .add("schema='" + schema + "'")
                .add("table='" + table + "'")
                .add("ts=" + ts)
                .add("opTime=" + opTime)
                .toString();
    }
}
