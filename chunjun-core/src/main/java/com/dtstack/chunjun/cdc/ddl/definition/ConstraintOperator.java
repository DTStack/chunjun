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

public class ConstraintOperator extends DdlOperator {

    private final TableIdentifier tableIdentifier;
    private final ConstraintDefinition constraintDefinition;

    private final Boolean enforced;

    private final String newName;

    public ConstraintOperator(
            EventType type,
            String sql,
            TableIdentifier tableIdentifier,
            ConstraintDefinition constraintDefinition) {
        this(type, sql, tableIdentifier, constraintDefinition, null, null);
    }

    public ConstraintOperator(
            EventType type,
            String sql,
            TableIdentifier tableIdentifier,
            ConstraintDefinition constraintDefinition,
            Boolean enforced,
            String newName) {
        super(type, sql);
        this.tableIdentifier = tableIdentifier;
        this.constraintDefinition = constraintDefinition;
        this.enforced = enforced;
        this.newName = newName;
        Preconditions.checkArgument(
                getSupportEventType().contains(type),
                "OperateConstraintDefinition not support type" + type);
    }

    List<EventType> getSupportEventType() {
        return ImmutableList.of(
                EventType.DROP_CONSTRAINT,
                EventType.ADD_CONSTRAINT,
                EventType.ALTER_CONSTRAINT_ENFORCED,
                EventType.RENAME_CONSTRAINT);
    }

    public TableIdentifier getTableIdentifier() {
        return tableIdentifier;
    }

    public ConstraintDefinition getConstraintDefinition() {
        return constraintDefinition;
    }

    public Boolean getEnforced() {
        return enforced;
    }

    public String getNewName() {
        return newName;
    }
}
