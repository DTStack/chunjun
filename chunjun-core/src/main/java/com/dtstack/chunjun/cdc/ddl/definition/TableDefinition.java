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

import java.util.List;
import java.util.Objects;

public class TableDefinition {
    /** 表路径* */
    private final TableIdentifier tableIdentifier;
    /** 字段信息* */
    private final List<ColumnDefinition> columnList;
    /** 索引信息* */
    private final List<IndexDefinition> indexList;
    /** 约束信息* */
    private final List<ConstraintDefinition> constraintList;

    /** 是否是临时* */
    private final boolean isTemporary;
    /** if not exists* */
    private final boolean ifNotExists;

    private String comment;

    public TableDefinition(
            TableIdentifier tableIdentifier,
            List<ColumnDefinition> columnList,
            List<IndexDefinition> indexList,
            List<ConstraintDefinition> constraintList,
            boolean isTemporary,
            boolean ifNotExists,
            String comment) {
        this.tableIdentifier = tableIdentifier;
        this.columnList = columnList;
        this.indexList = indexList;
        this.constraintList = constraintList;
        this.isTemporary = isTemporary;
        this.ifNotExists = ifNotExists;
        this.comment = comment;
    }

    public TableIdentifier getTableIdentifier() {
        return tableIdentifier;
    }

    public List<ColumnDefinition> getColumnList() {
        return columnList;
    }

    public List<IndexDefinition> getIndexList() {
        return indexList;
    }

    public List<ConstraintDefinition> getConstraintList() {
        return constraintList;
    }

    public boolean isTemporary() {
        return isTemporary;
    }

    public boolean isIfNotExists() {
        return ifNotExists;
    }

    public String getComment() {
        return comment;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TableDefinition)) return false;
        TableDefinition that = (TableDefinition) o;
        return isTemporary == that.isTemporary
                && ifNotExists == that.ifNotExists
                && Objects.equals(tableIdentifier, that.tableIdentifier)
                && Objects.equals(comment, that.comment)
                && Objects.equals(columnList, that.columnList)
                && Objects.equals(indexList, that.indexList)
                && Objects.equals(constraintList, that.constraintList);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                tableIdentifier,
                columnList,
                indexList,
                constraintList,
                isTemporary,
                ifNotExists,
                comment);
    }
}
