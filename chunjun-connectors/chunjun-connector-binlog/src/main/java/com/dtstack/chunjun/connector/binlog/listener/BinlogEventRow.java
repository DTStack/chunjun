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
package com.dtstack.chunjun.connector.binlog.listener;

import com.alibaba.otter.canal.protocol.CanalEntry;

import java.io.Serializable;

public class BinlogEventRow implements Serializable {
    private static final long serialVersionUID = 1L;

    private final CanalEntry.RowChange rowChange;
    private final String schema;
    private final String table;
    private final String lsn;
    private final long executeTime;

    public BinlogEventRow(
            CanalEntry.RowChange rowChange,
            String schema,
            String table,
            long executeTime,
            String lsn) {
        this.rowChange = rowChange;
        this.schema = schema;
        this.table = table;
        this.executeTime = executeTime;
        this.lsn = lsn;
    }

    public CanalEntry.RowChange getRowChange() {
        return rowChange;
    }

    public String getSchema() {
        return schema;
    }

    public String getTable() {
        return table;
    }

    public long getExecuteTime() {
        return executeTime;
    }

    public String getLsn() {
        return lsn;
    }

    @Override
    public String toString() {
        return "BinlogEventRow{"
                + "RowChange="
                + rowChange
                + ", schema='"
                + schema
                + '\''
                + ", table='"
                + table
                + '\''
                + ", executeTime="
                + executeTime
                + '\''
                + ", lsn="
                + lsn
                + '}';
    }
}
