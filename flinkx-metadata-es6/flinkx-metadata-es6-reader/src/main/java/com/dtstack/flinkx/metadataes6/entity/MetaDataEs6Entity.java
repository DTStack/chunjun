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
package com.dtstack.flinkx.metadataes6.entity;

import com.dtstack.flinkx.metadata.entity.MetadataEntity;

import java.util.List;

/**
 * @author : baiyu
 * @date : 2020/12/4
 */
public class MetaDataEs6Entity extends MetadataEntity {

    /**
     * 索引配置参数
     */
    private IndexProperties indexProperties;

    /**
     * 字段列表
     */
    private List<ColumnEntity> column;

    private String indexName;

    /**
     * 索引别名列表
     */
    private List<AliasEntity> aliasList;

    public IndexProperties getIndexProperties() {
        return indexProperties;
    }

    public void setIndexProperties(IndexProperties indexProperties) {
        this.indexProperties = indexProperties;
    }

    public List<ColumnEntity> getColumn() {
        return column;
    }

    public void setColumn(List<ColumnEntity> column) {
        this.column = column;
    }

    public String getIndexName() {
        return indexName;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    public List<AliasEntity> getAlias() {
        return aliasList;
    }

    public void setAlias(List<AliasEntity> aliasList) {
        this.aliasList = aliasList;
    }
}
