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

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.dtstack.chunjun.connector.sqlservercdc.entity;

import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;

public class ChangeTable {

    private static final String CDC_SCHEMA = "cdc";
    private final String captureInstance;
    private final TableId sourceTableId;
    private final TableId changeTableId;
    private final Lsn startLsn;
    private Lsn stopLsn;
    private final List<String> columnList;
    private final int changeTableObjectId;

    public ChangeTable(
            TableId sourceTableId,
            String captureInstance,
            int changeTableObjectId,
            Lsn startLsn,
            Lsn stopLsn,
            List<String> columnList) {
        super();
        this.sourceTableId = sourceTableId;
        this.captureInstance = captureInstance;
        this.changeTableObjectId = changeTableObjectId;
        this.startLsn = startLsn;
        this.stopLsn = stopLsn;
        this.columnList = columnList;
        this.changeTableId =
                sourceTableId != null
                        ? new TableId(
                                sourceTableId.getCatalogName(), CDC_SCHEMA, captureInstance + "_CT")
                        : null;
    }

    public String getCaptureInstance() {
        return captureInstance;
    }

    public Lsn getStartLsn() {
        return startLsn;
    }

    public Lsn getStopLsn() {
        return stopLsn;
    }

    public void setStopLsn(Lsn stopLsn) {
        this.stopLsn = stopLsn;
    }

    public TableId getSourceTableId() {
        return sourceTableId;
    }

    public List<String> getColumnList() {
        return columnList;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ChangeTable that = (ChangeTable) o;
        return changeTableObjectId == that.changeTableObjectId
                && Objects.equals(captureInstance, that.captureInstance)
                && Objects.equals(sourceTableId, that.sourceTableId)
                && Objects.equals(changeTableId, that.changeTableId)
                && Objects.equals(startLsn, that.startLsn)
                && Objects.equals(stopLsn, that.stopLsn)
                && Objects.equals(columnList, that.columnList);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                captureInstance,
                sourceTableId,
                changeTableId,
                startLsn,
                stopLsn,
                columnList,
                changeTableObjectId);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", ChangeTable.class.getSimpleName() + "[", "]")
                .add("captureInstance='" + captureInstance + "'")
                .add("sourceTableId=" + sourceTableId)
                .add("changeTableId=" + changeTableId)
                .add("startLsn=" + startLsn)
                .add("stopLsn=" + stopLsn)
                .add("columnList=" + columnList)
                .add("changeTableObjectId=" + changeTableObjectId)
                .toString();
    }
}
