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

import com.dtstack.chunjun.cdc.ddl.ColumnType;

import java.util.Objects;

public class ColumnDefinition {
    /** 字段名称 */
    private final String name;
    /** 字段类型 */
    private final ColumnType type;
    /** 字段是否可以为空 */
    private final Boolean nullable;
    /** 是否是主键 */
    private final Boolean isPrimary;
    /** 是否是唯一键* */
    private final Boolean isUnique;
    /** 是否是递增 * */
    private final Boolean autoIncrement;
    /** 字段默认值 */
    private final String defaultValue;

    private final DataSourceFunction defaultFunction;
    /** 字段描述 */
    private final String comment;
    /** 长度 */
    private final Integer precision;
    /** 小数位数 */
    private final Integer scale;

    private final String check;

    public ColumnDefinition(
            String name,
            ColumnType type,
            Boolean nullable,
            Boolean isPrimary,
            Boolean isUnique,
            Boolean autoIncrement,
            String defaultValue,
            DataSourceFunction defaultFunction,
            String comment,
            Integer precision,
            Integer scale,
            String check) {
        this.name = name;
        this.type = type;
        this.nullable = nullable;
        this.isPrimary = isPrimary;
        this.isUnique = isUnique;
        this.autoIncrement = autoIncrement;
        this.defaultValue = defaultValue;
        this.comment = comment;
        this.precision = precision;
        this.scale = scale;
        this.check = check;
        this.defaultFunction = defaultFunction;
    }

    public String getName() {
        return name;
    }

    public ColumnType getType() {
        return type;
    }

    public Boolean isNullable() {
        return nullable;
    }

    public Boolean isPrimary() {
        return isPrimary;
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    public String getComment() {
        return comment;
    }

    public Integer getPrecision() {
        return precision;
    }

    public Integer getScale() {
        return scale;
    }

    public Boolean isUnique() {
        return isUnique;
    }

    public Boolean getAutoIncrement() {
        return autoIncrement;
    }

    public String getCheck() {
        return check;
    }

    public DataSourceFunction getDefaultFunction() {
        return defaultFunction;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ColumnDefinition)) return false;
        ColumnDefinition that = (ColumnDefinition) o;
        return Objects.equals(name, that.name)
                && Objects.equals(type, that.type)
                && Objects.equals(nullable, that.nullable)
                && Objects.equals(isPrimary, that.isPrimary)
                && Objects.equals(isUnique, that.isUnique)
                && Objects.equals(autoIncrement, that.autoIncrement)
                && Objects.equals(defaultValue, that.defaultValue)
                && defaultFunction == that.defaultFunction
                && Objects.equals(comment, that.comment)
                && Objects.equals(precision, that.precision)
                && Objects.equals(scale, that.scale)
                && Objects.equals(check, that.check);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                name,
                type,
                nullable,
                isPrimary,
                isUnique,
                autoIncrement,
                defaultValue,
                defaultFunction,
                comment,
                precision,
                scale,
                check);
    }
}
