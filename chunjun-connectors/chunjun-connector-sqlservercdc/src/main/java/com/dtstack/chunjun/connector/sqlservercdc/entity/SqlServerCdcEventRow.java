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

import java.io.Serializable;
import java.util.List;

public class SqlServerCdcEventRow implements Serializable {

    private static final long serialVersionUID = 1L;

    private String type;

    private String schema;

    private String table;

    private String lsn;

    private Long ts;

    private ChangeTable changeTable;

    private Object[] data;

    private Object[] dataPrev;

    private List<String> columnTypes;

    public SqlServerCdcEventRow(
            String type,
            String schema,
            String table,
            String lsn,
            Long ts,
            ChangeTable changeTable,
            Object[] data,
            Object[] dataPrev,
            List<String> types) {
        this.type = type;
        this.schema = schema;
        this.table = table;
        this.lsn = lsn;
        this.ts = ts;
        this.changeTable = changeTable;
        this.data = data;
        this.dataPrev = dataPrev;
        this.columnTypes = types;
    }

    public List<String> getColumnTypes() {
        return columnTypes;
    }

    public void setColumnTypes(List<String> columnTypes) {
        this.columnTypes = columnTypes;
    }

    public Object[] getData() {
        return data;
    }

    public void setData(Object[] data) {
        this.data = data;
    }

    public Object[] getDataPrev() {
        return dataPrev;
    }

    public void setDataPrev(Object[] dataPrev) {
        this.dataPrev = dataPrev;
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

    public String getLsn() {
        return lsn;
    }

    public void setLsn(String lsn) {
        this.lsn = lsn;
    }

    public Long getTs() {
        return ts;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }

    public ChangeTable getChangeTable() {
        return changeTable;
    }

    public void setChangeTable(ChangeTable changeTable) {
        this.changeTable = changeTable;
    }
}
