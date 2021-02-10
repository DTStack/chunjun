package com.dtstack.flinkx.metadatahbase.entity;

import java.io.Serializable;

/**
 * @company:www.dtstack.com
 * @Author:shiFang
 * @Date:2021-01-18 19:35
 * @Description:
 */
public class HbaseTableEntity implements Serializable {

    private Integer regionCount;

    private String tableName;

    private String nameSpace;

    private Long createTime;

    private Long totalSize;

    public void setRegionCount(Integer regionCount) {
        this.regionCount = regionCount;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public void setNameSpace(String nameSpace) {
        this.nameSpace = nameSpace;
    }

    public void setCreateTime(Long createTime) {
        this.createTime = createTime;
    }

    public void setTotalSize(Long totalSize) {
        this.totalSize = totalSize;
    }
}
