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

package com.dtstack.flinkx.metadatahbase.entity;

import java.io.Serializable;

/**
 * @company:www.dtstack.com
 * @Author:shiFang
 * @Date:2021-01-18 19:35
 * @Description:
 */
public class HbaseTableEntity implements Serializable {

    protected static final long serialVersionUID = 1L;

    /**region的数量*/
    private Integer regionCount;

    /**表名称*/
    private String tableName;

    /**hbase namespace*/
    private String namespace;

    /**hbase 建表时间*/
    private Long createTime;

    /**hbase 表数量大小*/
    private Long totalSize;

    public void setRegionCount(Integer regionCount) {
        this.regionCount = regionCount;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public void setCreateTime(Long createTime) {
        this.createTime = createTime;
    }

    public void setTotalSize(Long totalSize) {
        this.totalSize = totalSize;
    }

    @Override
    public String toString() {
        return "HbaseTableEntity{" +
                "regionCount=" + regionCount +
                ", tableName='" + tableName + '\'' +
                ", nameSpace='" + namespace + '\'' +
                ", createTime=" + createTime +
                ", totalSize=" + totalSize +
                '}';
    }
}
