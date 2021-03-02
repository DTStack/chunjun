package com.dtstack.flinkx.metadatasqlserver.entity;

import com.dtstack.metadata.rdb.core.entity.TableEntity;

import java.util.List;

/**
 * Company：www.dtstack.com
 *
 * @author shitou
 * @date 2021/2/1 11:30
 */
public class SqlserverTableEntity extends TableEntity {

    /**主键名*/
    private List<String> primaryKey;

    /**索引*/
    private List<SqlserverIndexEntity> index;

    /**分区信息*/
    private List<SqlserverPartitionEntity> partition;

    public List<String> getPrimaryKey() {
        return primaryKey;
    }

    public void setPrimaryKey(List<String> primaryKey) {
        this.primaryKey = primaryKey;
    }

    public List<SqlserverIndexEntity> getIndex() {
        return index;
    }

    public void setIndex(List<SqlserverIndexEntity> index) {
        this.index = index;
    }

    public List<SqlserverPartitionEntity> getPartition() {
        return partition;
    }

    public void setPartition(List<SqlserverPartitionEntity> partition) {
        this.partition = partition;
    }

    @Override
    public String toString() {
        return "SqlserverTableEntity{" +
                "primaryKey=" + primaryKey +
                ", index=" + index +
                ", partition=" + partition +
                ", tableName='" + tableName + '\'' +
                ", createTime='" + createTime + '\'' +
                ", comment='" + comment + '\'' +
                ", totalSize=" + totalSize +
                ", rows=" + rows +
                '}';
    }
}
