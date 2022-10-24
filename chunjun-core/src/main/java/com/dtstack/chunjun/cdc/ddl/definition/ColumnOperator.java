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

public class ColumnOperator extends DdlOperator {
    /** 修改表路径 * */
    private final TableIdentifier tableIdentifier;
    /** 增加的字段信息 * */
    private final List<ColumnDefinition> columns;

    private final Boolean dropDefault;

    private final Boolean setDefault;

    private final String newName;

    public ColumnOperator(
            EventType type,
            String sql,
            TableIdentifier tableIdentifier,
            List<ColumnDefinition> columns,
            Boolean dropDefault,
            Boolean setDefault,
            String newName) {
        super(type, sql);
        this.tableIdentifier = tableIdentifier;
        this.columns = columns;
        this.dropDefault = dropDefault;
        this.setDefault = setDefault;
        this.newName = newName;
        Preconditions.checkArgument(
                getSupportEventType().contains(type),
                "OperatorColumnDefinition not support type" + type);
    }

    public TableIdentifier getTableIdentifier() {
        return tableIdentifier;
    }

    public List<ColumnDefinition> getColumns() {
        return columns;
    }

    public List<EventType> getSupportEventType() {
        return ImmutableList.of(
                EventType.ADD_COLUMN,
                EventType.DROP_COLUMN,
                EventType.RENAME_COLUMN,
                EventType.ALTER_COLUMN);
    }

    public boolean isDropDefault() {
        return dropDefault != null && dropDefault;
    }

    public boolean isSetDefault() {
        return setDefault != null && setDefault;
    }

    public String getNewName() {
        return newName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ColumnOperator)) return false;
        ColumnOperator that = (ColumnOperator) o;
        return Objects.equals(tableIdentifier, that.tableIdentifier)
                && Objects.equals(columns, that.columns)
                && Objects.equals(dropDefault, that.dropDefault)
                && Objects.equals(newName, that.newName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableIdentifier, columns, dropDefault, newName);
    }

    @Override
    public String toString() {
        return "ColumnOperator{"
                + "tableIdentifier="
                + tableIdentifier
                + ", columns="
                + columns
                + ", dropDefault="
                + dropDefault
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
}
