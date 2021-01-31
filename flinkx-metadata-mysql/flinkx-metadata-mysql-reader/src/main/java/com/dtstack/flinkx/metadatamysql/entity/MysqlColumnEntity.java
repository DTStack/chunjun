package com.dtstack.flinkx.metadatamysql.entity;

import com.dtstack.metadata.rdb.core.entity.ColumnEntity;

/**
 * @company:www.dtstack.com
 * @Author:shiFang
 * @Date:2021-01-29 11:11
 * @Description:
 */
public class MysqlColumnEntity extends ColumnEntity {

    private String primaryKey;

    private String partition;

    public String getPrimaryKey() {
        return primaryKey;
    }

    public void setPrimaryKey(String primaryKey) {
        this.primaryKey = primaryKey;
    }

    public String getPartition() {
        return partition;
    }

    public void setPartition(String partition) {
        this.partition = partition;
    }
}
