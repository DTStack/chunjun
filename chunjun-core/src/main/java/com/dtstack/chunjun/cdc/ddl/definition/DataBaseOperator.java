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

public class DataBaseOperator extends DdlOperator {
    /** 数据库名称* */
    private final String name;

    public DataBaseOperator(EventType type, String sql, String name) {
        super(type, sql);
        this.name = name;
        Preconditions.checkArgument(
                getSupportEventType().contains(type),
                "OperateDataBaseDefinition not support type" + type);
    }

    public String getName() {
        return name;
    }

    List<EventType> getSupportEventType() {
        return ImmutableList.of(EventType.CREATE_DATABASE, EventType.DROP_DATABASE);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DataBaseOperator)) return false;
        DataBaseOperator that = (DataBaseOperator) o;
        return Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }
}
