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

package com.dtstack.chunjun.connector.starrocks.source.be.entity;

import org.apache.flink.table.types.logical.LogicalTypeRoot;

import java.util.StringJoiner;

public class ColumnInfo {
    private String fieldName;
    private LogicalTypeRoot logicalTypeRoot;
    private String starRocksType;
    private int index;

    public ColumnInfo(
            String fieldName, LogicalTypeRoot logicalTypeRoot, String starRocksType, int index) {
        this.fieldName = fieldName;
        this.logicalTypeRoot = logicalTypeRoot;
        this.starRocksType = starRocksType;
        this.index = index;
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public LogicalTypeRoot getLogicalTypeRoot() {
        return logicalTypeRoot;
    }

    public void setLogicalTypeRoot(LogicalTypeRoot logicalTypeRoot) {
        this.logicalTypeRoot = logicalTypeRoot;
    }

    public String getStarRocksType() {
        return starRocksType;
    }

    public void setStarRocksType(String starRocksType) {
        this.starRocksType = starRocksType;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", ColumnInfo.class.getSimpleName() + "[", "]")
                .add("fieldName='" + fieldName + "'")
                .add("logicalTypeRoot=" + logicalTypeRoot)
                .add("starRocksType='" + starRocksType + "'")
                .add("index=" + index)
                .toString();
    }
}
