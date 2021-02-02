package com.dtstack.flinkx.metadatasqlserver.entity;

import java.io.Serializable;

/**
 * Company：www.dtstack.com
 *
 * @author shitou
 * @date 2021/2/1 16:37
 */
public class SqlserverPartitionEntity implements Serializable {

    /**
     * 分区字段
     */
    private String columnName;

    /**
     * 分区表的数据量
     */
    private long rows;

    /**
     * 创建时间
     */
    private String createTime;

    /**
     *文件组名
     */
    private String fileGroupName;

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public long getRows() {
        return rows;
    }

    public void setRows(long rows) {
        this.rows = rows;
    }

    public String getCreateTime() {
        return createTime;
    }

    public void setCreateTime(String createTime) {
        this.createTime = createTime;
    }

    public String getFileGroupName() {
        return fileGroupName;
    }

    public void setFileGroupName(String fileGroupName) {
        this.fileGroupName = fileGroupName;
    }
}
