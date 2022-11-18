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
                "OperateTableDefinition not support type " + type);
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

    public static class Builder {
        private EventType type = null;
        private String sql = null;
        private TableIdentifier tableIdentifier = null;
        private List<ColumnDefinition> columnList = null;
        private List<IndexDefinition> indexList = null;
        private String comment = null;
        private List<ConstraintDefinition> constraintList = null;
        private boolean isTemporary = false;
        private boolean ifNotExists = false;
        private boolean likeTable = false;
        private TableIdentifier likeTableIdentifier = null;
        private TableIdentifier newTableIdentifier = null;

        public Builder type(EventType type) {
            this.type = type;
            return this;
        }

        public Builder sql(String sql) {
            this.sql = sql;
            return this;
        }

        public Builder tableIdentifier(TableIdentifier tableIdentifier) {
            this.tableIdentifier = tableIdentifier;
            return this;
        }

        public Builder columnList(List<ColumnDefinition> columnList) {
            this.columnList = columnList;
            return this;
        }

        public Builder indexList(List<IndexDefinition> indexList) {
            this.indexList = indexList;
            return this;
        }

        public Builder comment(String comment) {
            this.comment = comment;
            return this;
        }

        public Builder constraintList(List<ConstraintDefinition> constraintList) {
            this.constraintList = constraintList;
            return this;
        }

        public Builder isTemporary(boolean isTemporary) {
            this.isTemporary = isTemporary;
            return this;
        }

        public Builder ifNotExists(boolean ifNotExists) {
            this.ifNotExists = ifNotExists;
            return this;
        }

        public Builder likeTable(boolean likeTable) {
            this.likeTable = likeTable;
            return this;
        }

        public Builder likeTableIdentifier(TableIdentifier likeTableIdentifier) {
            this.likeTableIdentifier = likeTableIdentifier;
            return this;
        }

        public Builder newTableIdentifier(TableIdentifier newTableIdentifier) {
            this.newTableIdentifier = newTableIdentifier;
            return this;
        }

        public TableOperator build() {
            return new TableOperator(
                    type,
                    sql,
                    tableIdentifier,
                    columnList,
                    indexList,
                    comment,
                    constraintList,
                    isTemporary,
                    ifNotExists,
                    likeTable,
                    likeTableIdentifier,
                    newTableIdentifier);
        }
    }
}
