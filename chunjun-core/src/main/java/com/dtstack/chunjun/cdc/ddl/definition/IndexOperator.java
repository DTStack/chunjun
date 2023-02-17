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

public class IndexOperator extends DdlOperator {
    private final TableIdentifier tableIdentifier;
    /** index信息 * */
    private final IndexDefinition index;

    private final String newName;

    public IndexOperator(
            EventType type,
            String sql,
            TableIdentifier tableIdentifier,
            IndexDefinition index,
            String newName) {
        super(type, sql);
        this.tableIdentifier = tableIdentifier;
        this.index = index;
        this.newName = newName;
        Preconditions.checkArgument(
                getSupportEventType().contains(type),
                "OperateConstraintDefinition not support type" + type);
    }

    public TableIdentifier getTableIdentifier() {
        return tableIdentifier;
    }

    public IndexDefinition getIndex() {
        return index;
    }

    public String getNewName() {
        return newName;
    }

    List<EventType> getSupportEventType() {
        return ImmutableList.of(EventType.ADD_INDEX, EventType.DROP_INDEX, EventType.RENAME_INDEX);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof IndexOperator)) return false;
        IndexOperator that = (IndexOperator) o;
        return Objects.equals(tableIdentifier, that.tableIdentifier)
                && Objects.equals(index, that.index);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableIdentifier, index);
    }

    @Override
    public String toString() {
        return "IndexOperator{"
                + "tableIdentifier="
                + tableIdentifier
                + ", index="
                + index
                + ", newName='"
                + newName
                + '\''
                + ", type="
                + type
                + ", sql='"
                + sql
                + '\''
                + '}';
    }

    public static class Builder {
        private EventType type = null;
        private String sql = null;
        private TableIdentifier tableIdentifier = null;
        private IndexDefinition index = null;
        private String newName = null;

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

        public Builder index(IndexDefinition index) {
            this.index = index;
            return this;
        }

        public Builder newName(String newName) {
            this.newName = newName;
            return this;
        }

        public IndexOperator build() {
            return new IndexOperator(type, sql, tableIdentifier, index, newName);
        }
    }
}
