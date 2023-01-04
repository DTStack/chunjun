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

import lombok.Data;

import java.util.List;

@Data
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
}
