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

public class ConstraintDefinition {

    /** 约束名称* */
    private final String name;

    /** 主键约束* */
    private final Boolean isPrimary;

    /** 唯一约束* */
    private final Boolean isUnique;

    /** 是否是检查约束 * */
    private final Boolean isCheck;

    /** 约束字段* */
    private List<String> columns;

    /** 约束表达式* */
    private final String check;

    /** 注释* */
    private final String comment;

    public ConstraintDefinition(
            String name,
            Boolean isPrimary,
            Boolean isUnique,
            Boolean isCheck,
            List<String> columns,
            String check,
            String comment) {
        this.name = name;
        this.isPrimary = isPrimary;
        this.isUnique = isUnique;
        this.isCheck = isCheck;
        this.columns = columns;
        this.check = check;
        this.comment = comment;
    }

    public String getName() {
        return name;
    }

    public boolean isPrimary() {
        return isPrimary != null && isPrimary;
    }

    public boolean isUnique() {
        return isUnique != null && isUnique;
    }

    public Boolean isCheck() {
        return isCheck != null && isCheck;
    }

    public List<String> getColumns() {
        return columns;
    }

    public String getCheck() {
        return check;
    }

    public String getComment() {
        return comment;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ConstraintDefinition)) return false;
        ConstraintDefinition that = (ConstraintDefinition) o;
        return Objects.equals(name, that.name)
                && Objects.equals(isPrimary, that.isPrimary)
                && Objects.equals(isUnique, that.isUnique)
                && Objects.equals(isCheck, that.isCheck)
                && Objects.equals(columns, that.columns)
                && Objects.equals(check, that.check)
                && Objects.equals(comment, that.comment);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, isPrimary, isUnique, isCheck, columns, check, comment);
    }

    public void setColumns(List<String> columnList) {
        this.columns = columnList;
    }

    public static class Builder {
        private String name = null;
        private Boolean isPrimary = false;
        private Boolean isUnique = false;
        private Boolean isCheck = false;
        private List<String> columns = null;
        private String check = null;
        private String comment = null;

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder isPrimary(Boolean primary) {
            isPrimary = primary;
            return this;
        }

        public Builder isUnique(Boolean unique) {
            isUnique = unique;
            return this;
        }

        public Builder isCheck(Boolean check) {
            isCheck = check;
            return this;
        }

        public Builder columns(List<String> columns) {
            this.columns = columns;
            return this;
        }

        public Builder check(String check) {
            this.check = check;
            return this;
        }

        public Builder comment(String comment) {
            this.comment = comment;
            return this;
        }

        public ConstraintDefinition build() {
            return new ConstraintDefinition(
                    name, isPrimary, isUnique, isCheck, columns, check, comment);
        }
    }
}
