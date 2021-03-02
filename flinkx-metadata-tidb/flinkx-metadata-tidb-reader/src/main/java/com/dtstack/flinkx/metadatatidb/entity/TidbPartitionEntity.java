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

package com.dtstack.flinkx.metadatatidb.entity;

import java.io.Serializable;

/**
 * @company:www.dtstack.com
 * @Author:shiFang
 * @Date:2021-01-29 11:33
 * @Description:
 */
public class TidbPartitionEntity implements Serializable {

    protected static final long serialVersionUID = 1L;

    /**字段名称*/
    private String columnName;

    /**创建时间*/
    private String createTime;

    /**分区大小*/
    private Long partitionSize;

    /**分区数据行数*/
    private Long partitionRows;

    /**修改时间*/
    private String updateTime;

    public String getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(String updateTime) {
        this.updateTime = updateTime;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getCreateTime() {
        return createTime;
    }

    public void setCreateTime(String createTime) {
        this.createTime = createTime;
    }

    public Long getPartitionSize() {
        return partitionSize;
    }

    public void setPartitionSize(Long partitionSize) {
        this.partitionSize = partitionSize;
    }

    public Long getPartitionRows() {
        return partitionRows;
    }

    public void setPartitionRows(Long partitionRows) {
        this.partitionRows = partitionRows;
    }

    @Override
    public String toString() {
        return "TidbPartitionEntity{" +
                "columnName='" + columnName + '\'' +
                ", createTime='" + createTime + '\'' +
                ", partitionSize=" + partitionSize +
                ", partitionRows=" + partitionRows +
                ", updateTime='" + updateTime + '\'' +
                '}';
    }
}
