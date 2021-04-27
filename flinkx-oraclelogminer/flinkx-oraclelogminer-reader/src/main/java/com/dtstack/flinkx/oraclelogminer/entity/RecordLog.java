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

package com.dtstack.flinkx.oraclelogminer.entity;

import java.math.BigDecimal;

public class RecordLog {

    private BigDecimal scn;
    private String sqlUndo;
    private String sqlRedo;
    private String xidSqn;
    private String rowId;
    private String tableName;
    private boolean hasMultiSql;
    private int operationCode;

    public RecordLog(BigDecimal scn, String sqlUndo, String sqlRedo, String xidSqn, String rowId, int operationCode, boolean hasMultiSql, String tableName) {
        this.scn = scn;
        this.sqlUndo = sqlUndo;
        this.sqlRedo = sqlRedo;
        this.xidSqn = xidSqn;
        this.rowId = rowId;
        this.operationCode = operationCode;
        this.hasMultiSql = hasMultiSql;
        this.tableName = tableName;
    }

    public BigDecimal getScn() {
        return scn;
    }

    public void setScn(BigDecimal scn) {
        this.scn = scn;
    }

    public String getSqlUndo() {
        return sqlUndo;
    }

    public void setSqlUndo(String sqlUndo) {
        this.sqlUndo = sqlUndo;
    }

    public String getSqlRedo() {
        return sqlRedo;
    }

    public void setSqlRedo(String sqlRedo) {
        this.sqlRedo = sqlRedo;
    }

    public int getOperationCode() {
        return operationCode;
    }

    public void setOperationCode(int operationCode) {
        this.operationCode = operationCode;
    }

    public String getXidSqn() {
        return xidSqn;
    }

    public void setXidSqn(String xidSqn) {
        this.xidSqn = xidSqn;
    }

    public String getRowId() {
        return rowId;
    }

    public void setRowId(String rowId) {
        this.rowId = rowId;
    }

    public boolean getHasMultiSql() {
        return hasMultiSql;
    }

    public void setHasMultiSql(boolean hasMultiSql) {
        this.hasMultiSql = hasMultiSql;
    }


    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public String toString() {
        return "RecordLog{" +
                "scn=" + scn +
                ", sqlUndo='" + sqlUndo + '\'' +
                ", sqlRedo='" + sqlRedo + '\'' +
                ", xidSqn='" + xidSqn + '\'' +
                ", rowId='" + rowId + '\'' +
                ", tableName='" + tableName + '\'' +
                ", hasMultiSql=" + hasMultiSql +
                ", operationCode=" + operationCode +
                '}';
    }
}
