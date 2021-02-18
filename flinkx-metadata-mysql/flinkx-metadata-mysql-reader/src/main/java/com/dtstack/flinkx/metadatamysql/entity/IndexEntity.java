/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.flinkx.metadatamysql.entity;

import com.google.gson.annotations.SerializedName;

import java.io.Serializable;

/**
 * @author kunni@dtstack.com
 */
public class IndexEntity implements Serializable {

    /**索引名称*/
    protected String indexName;

    /**索引对应的字段名称*/
    protected String columnName;

    /**索引描述*/
    protected String indexComment;

    /**索引类型*/
    protected String indexType;

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public void setIndexComment(String indexComment) {
        this.indexComment = indexComment;
    }

    public void setIndexType(String indexType) {
        this.indexType = indexType;
    }

}
