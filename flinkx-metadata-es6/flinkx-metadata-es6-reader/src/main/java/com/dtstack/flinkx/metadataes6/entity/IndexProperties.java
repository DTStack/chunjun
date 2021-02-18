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

import java.io.Serializable;

/**
 * @author : baiyu
 * @date : 2020/12/4
 */
public class IndexProperties implements Serializable {

    /**索引存储的总容量*/
    private String totalSize;

    /**分片数*/
    private String shards;

    /**索引创建时间*/
    private String createTime;

    /**副本数量*/
    private String replicas;

    /**已删除文档数*/
    private String docsDeleted;

    /**文档数*/
    private String docsCount;

    /**主分片的总容量*/
    private String priSize;

    /**green为正常，yellow表示索引不可靠（单节点），red索引不可用*/
    private String health;

    /**索引的唯一标识*/
    private String uuid;

    /**表明索引是否打开*/
    private String status;

    public String getTotalSize() {
        return totalSize;
    }

    public void setTotalSize(String totalSize) {
        this.totalSize = totalSize;
    }

    public String getShards() {
        return shards;
    }

    public void setShards(String shards) {
        this.shards = shards;
    }

    public String getCreateTime() {
        return createTime;
    }

    public void setCreateTime(String createTime) {
        this.createTime = createTime;
    }

    public String getReplicas() {
        return replicas;
    }

    public void setReplicas(String replicas) {
        this.replicas = replicas;
    }

    public String getDocsDeleted() {
        return docsDeleted;
    }

    public void setDocsDeleted(String docsDeleted) {
        this.docsDeleted = docsDeleted;
    }

    public String getDocsCount() {
        return docsCount;
    }

    public void setDocsCount(String docsCount) {
        this.docsCount = docsCount;
    }

    public String getPriSize() {
        return priSize;
    }

    public void setPriSize(String priSize) {
        this.priSize = priSize;
    }

    public String getHealth() {
        return health;
    }

    public void setHealth(String health) {
        this.health = health;
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }
}
