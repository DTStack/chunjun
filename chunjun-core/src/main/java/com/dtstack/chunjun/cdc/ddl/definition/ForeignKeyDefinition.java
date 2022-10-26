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

public class ForeignKeyDefinition extends ConstraintDefinition {

    private final TableIdentifier referenceTable;
    private final List<String> referenceColumns;

    private final Constraint onDelete;
    private final Constraint onUpdate;

    public ForeignKeyDefinition(
            String name,
            List<String> columns,
            String comment,
            TableIdentifier referenceTable,
            List<String> referenceColumns,
            Constraint onDelete,
            Constraint onUpdate) {
        super(name, false, false, false, columns, null, comment);
        this.referenceTable = referenceTable;
        this.referenceColumns = referenceColumns;
        this.onDelete = onDelete;
        this.onUpdate = onUpdate;
    }

    public TableIdentifier getReferenceTable() {
        return referenceTable;
    }

    public List<String> getReferenceColumns() {
        return referenceColumns;
    }

    public Constraint getOnDelete() {
        return onDelete;
    }

    public Constraint getOnUpdate() {
        return onUpdate;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ForeignKeyDefinition)) return false;
        ForeignKeyDefinition that = (ForeignKeyDefinition) o;
        return Objects.equals(referenceTable, that.referenceTable)
                && Objects.equals(referenceColumns, that.referenceColumns)
                && onDelete == that.onDelete
                && onUpdate == that.onUpdate;
    }

    @Override
    public int hashCode() {
        return Objects.hash(referenceTable, referenceColumns, onDelete, onUpdate);
    }

    public enum Constraint {
        RESTRICT,
        CASCADE,
        SET_NULL,
        SET_DEFAULT,
        NO_ACTION
    }
}
