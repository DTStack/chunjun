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

package com.dtstack.chunjun.cdc.ddl.definition;

import com.dtstack.chunjun.cdc.EventType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

public class TableOperator extends DdlOperator {

    private final TableDefinition tableDefinition;

    /** create table like otherTable like语法信息* */
    private final boolean likeTable;
    /** other table * */
    private final TableIdentifier likeTableIdentifier;

    // for rename table
    private final TableIdentifier newTableIdentifier;

    public TableOperator(
            EventType type,
            String sql,
            TableIdentifier tableIdentifier,
            List<ColumnDefinition> columnList,
            List<IndexDefinition> indexList,
            String comment,
            List<ConstraintDefinition> constraintList,
            boolean isTemporary,
            boolean ifNotExists,
            boolean likeTable,
            TableIdentifier likeTableIdentifier,
            TableIdentifier newTableIdentifier) {
        super(type, sql);
        this.tableDefinition =
                new TableDefinition(
                        tableIdentifier,
                        columnList,
                        indexList,
                        constraintList,
                        isTemporary,
                        ifNotExists,
                        comment);
        this.likeTable = likeTable;
        this.likeTableIdentifier = likeTableIdentifier;
        this.newTableIdentifier = newTableIdentifier;
        Preconditions.checkArgument(
                getSupportEventType().contains(type),
                "OperateTableDefinition not support type" + type);
    }

    public boolean isLikeTable() {
        return likeTable;
    }

    public TableIdentifier getLikeTableIdentifier() {
        return likeTableIdentifier;
    }

    public TableDefinition getTableDefinition() {
        return tableDefinition;
    }

    public TableIdentifier getNewTableIdentifier() {
        return newTableIdentifier;
    }

    public List<EventType> getSupportEventType() {
        return ImmutableList.of(
                EventType.CREATE_TABLE,
                EventType.DROP_TABLE,
                EventType.RENAME_TABLE,
                EventType.ALTER_TABLE_COMMENT,
                EventType.TRUNCATE_TABLE);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TableOperator)) return false;
        TableOperator that = (TableOperator) o;
        return likeTable == that.likeTable
                && Objects.equals(tableDefinition, that.tableDefinition)
                && Objects.equals(likeTableIdentifier, that.likeTableIdentifier);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableDefinition, likeTable, likeTableIdentifier);
    }

    @Override
    public String toString() {
        return "TableOperator{"
                + "type="
                + type
                + ", sql='"
                + sql
                + '\''
                + ", tableDefinition="
                + tableDefinition
                + ", likeTable="
                + likeTable
                + ", likeTableIdentifier="
                + likeTableIdentifier
                + ", newTableIdentifier="
                + newTableIdentifier
                + '}';
    }
}
