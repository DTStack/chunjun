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

public class IndexDefinition {
    /** 索引类型 * */
    private final IndexType indexType;
    /** 索引名称 * */
    private final String indexName;
    /** 索引注释 * */
    private final String comment;
    /** 索引组成字段* */
    private final List<ColumnInfo> columns;

    /** 是否可见 * */
    private final Boolean isVisiable;

    public IndexDefinition(
            IndexType indexType,
            String indexName,
            String comment,
            List<ColumnInfo> columns,
            Boolean isVisiable) {
        this.indexType = indexType;
        this.indexName = indexName;
        this.comment = comment;
        this.columns = columns;
        this.isVisiable = isVisiable;
    }

    public IndexType getIndexType() {
        return indexType;
    }

    public String getIndexName() {
        return indexName;
    }

    public String getComment() {
        return comment;
    }

    public List<ColumnInfo> getColumns() {
        return columns;
    }

    public Boolean getVisiable() {
        return isVisiable;
    }

    public static class ColumnInfo {
        private String name;
        private Integer length;

        public ColumnInfo(String name, Integer length) {
            this.name = name;
            this.length = length;
        }

        public ColumnInfo(String name) {
            this(name, null);
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        public Integer getLength() {
            return length;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof IndexDefinition)) return false;
        IndexDefinition that = (IndexDefinition) o;
        return indexType == that.indexType
                && Objects.equals(indexName, that.indexName)
                && Objects.equals(comment, that.comment)
                && Objects.equals(columns, that.columns)
                && Objects.equals(isVisiable, that.isVisiable);
    }

    @Override
    public int hashCode() {
        return Objects.hash(indexType, indexName, comment, columns, isVisiable);
    }

    @Override
    public String toString() {
        return "IndexDefinition{"
                + "indexType="
                + indexType
                + ", indexName='"
                + indexName
                + '\''
                + ", comment='"
                + comment
                + '\''
                + ", columns="
                + columns
                + ", isVisiable="
                + isVisiable
                + '}';
    }

    public static class Builder {
        private IndexType indexType;
        private String indexName = null;
        private String comment = null;
        private List<ColumnInfo> columns = null;
        private Boolean isVisiable = false;

        public Builder indexType(IndexType indexType) {
            this.indexType = indexType;
            return this;
        }

        public Builder indexName(String indexName) {
            this.indexName = indexName;
            return this;
        }

        public Builder comment(String comment) {
            this.comment = comment;
            return this;
        }

        public Builder columns(List<ColumnInfo> columns) {
            this.columns = columns;
            return this;
        }

        public Builder visiable(Boolean visiable) {
            isVisiable = visiable;
            return this;
        }

        public IndexDefinition build() {
            return new IndexDefinition(indexType, indexName, comment, columns, isVisiable);
        }
    }
}
