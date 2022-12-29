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
    private String name;
    /** 字段类型 */
    private final ColumnType type;
    /** 字段是否可以为空 */
    private Boolean nullable;
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

    public void setName(String name) {
        this.name = name;
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

    public void setNullable(Boolean nullable) {
        this.nullable = nullable;
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

    @Override
    public String toString() {
        return "ColumnDefinition{"
                + "name='"
                + name
                + '\''
                + ", type="
                + type
                + ", nullable="
                + nullable
                + ", isPrimary="
                + isPrimary
                + ", isUnique="
                + isUnique
                + ", autoIncrement="
                + autoIncrement
                + ", defaultValue='"
                + defaultValue
                + '\''
                + ", defaultFunction="
                + defaultFunction
                + ", comment='"
                + comment
                + '\''
                + ", precision="
                + precision
                + ", scale="
                + scale
                + ", check='"
                + check
                + '\''
                + '}';
    }

    public static class Builder {
        private String name = null;
        private ColumnType type = null;
        private Boolean nullable = true;
        private Boolean isPrimary = false;
        private Boolean isUnique = false;
        private Boolean autoIncrement = false;
        private String defaultValue = null;
        private DataSourceFunction defaultFunction = null;
        private String comment = null;
        private Integer precision = null;
        private Integer scale = null;
        private String check = null;

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder type(ColumnType type) {
            this.type = type;
            return this;
        }

        public Builder nullable(Boolean nullable) {
            this.nullable = nullable;
            return this;
        }

        public Builder isPrimary(Boolean isPrimary) {
            this.isPrimary = isPrimary;
            return this;
        }

        public Builder isUnique(Boolean isUnique) {
            this.isUnique = isUnique;
            return this;
        }

        public Builder autoIncrement(Boolean autoIncrement) {
            this.autoIncrement = autoIncrement;
            return this;
        }

        public Builder defaultValue(String defaultValue) {
            this.defaultValue = defaultValue;
            return this;
        }

        public Builder defaultFunction(DataSourceFunction defaultFunction) {
            this.defaultFunction = defaultFunction;
            return this;
        }

        public Builder comment(String comment) {
            this.comment = comment;
            return this;
        }

        public Builder precision(Integer precision) {
            this.precision = precision;
            return this;
        }

        public Builder scale(Integer scale) {
            this.scale = scale;
            return this;
        }

        public Builder check(String check) {
            this.check = check;
            return this;
        }

        public ColumnDefinition build() {
            return new ColumnDefinition(
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
}
