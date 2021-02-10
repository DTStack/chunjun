package com.dtstack.flinkx.metadatatidb.entity;

import java.io.Serializable;

/**
 * @company:www.dtstack.com
 * @Author:shiFang
 * @Date:2021-01-29 11:33
 * @Description:
 */
public class TidbPartitionEntity implements Serializable {

    private String columnName;

    private String createTime;

    private Long partitionSize;

    private Long partitionRows;

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
}
