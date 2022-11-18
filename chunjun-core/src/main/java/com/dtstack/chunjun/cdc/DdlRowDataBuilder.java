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

package com.dtstack.chunjun.cdc;

/**
 * 构建DdlRowData，header顺序如下： database -> 0 | schema -> 1 | table ->2 | type -> 3 | sql -> 4 | lsn ->
 * 5 | lsn_sequence -> 6
 */
public class DdlRowDataBuilder {

    private static final String[] HEADERS = {
        "database", "schema", "table", "type", "content", "lsn", "lsn_sequence", "snapshot"
    };

    private final DdlRowData ddlRowData;

    private DdlRowDataBuilder() {
        ddlRowData = new DdlRowData(HEADERS);
    }

    public static DdlRowDataBuilder builder() {
        return new DdlRowDataBuilder();
    }

    public DdlRowDataBuilder setDatabaseName(String databaseName) {
        ddlRowData.setDdlInfo(0, databaseName);
        return this;
    }

    public DdlRowDataBuilder setSchemaName(String schemaName) {
        ddlRowData.setDdlInfo(1, schemaName);
        return this;
    }

    public DdlRowDataBuilder setTableName(String tableName) {
        ddlRowData.setDdlInfo(2, tableName);
        return this;
    }

    public DdlRowDataBuilder setType(String type) {
        ddlRowData.setDdlInfo(3, type);
        return this;
    }

    public DdlRowDataBuilder setContent(String content) {
        ddlRowData.setDdlInfo(4, content);
        return this;
    }

    public DdlRowDataBuilder setLsn(String lsn) {
        ddlRowData.setDdlInfo(5, lsn);
        return this;
    }

    public DdlRowDataBuilder setLsnSequence(String lsnSequence) {
        ddlRowData.setDdlInfo(6, lsnSequence);
        return this;
    }

    public DdlRowDataBuilder setSnapShot(Boolean snapShot) {
        ddlRowData.setDdlInfo(7, snapShot.toString());
        return this;
    }

    public DdlRowData build() {
        return ddlRowData;
    }
}
