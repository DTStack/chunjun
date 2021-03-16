package com.dtstack.flinkx.metadatapostgresql.entity;


import com.dtstack.metadata.rdb.core.entity.TableEntity;

import java.util.List;

/**
 * 表的元数据
 * @author shitou
 * @date 2020/12/10 15:25
 */
public class TableMetaData extends TableEntity {

    /**表中的索引*/
    private List<IndexMetaData> indexes;

    /**主键*/
    private List<String> primaryKey;


    public TableMetaData() {

    }

    public List<IndexMetaData> getIndexes() {
        return indexes;
    }

    public void setIndexes(List<IndexMetaData> indexes) {
        this.indexes = indexes;
    }

    public List<String> getPrimaryKey() {
        return primaryKey;
    }

    public void setPrimaryKey(List<String> primaryKey) {
        this.primaryKey = primaryKey;
    }

    @Override
    public String toString() {
        return "TableMetaData{" +
                "indexes=" + indexes +
                ", primaryKey=" + primaryKey +
                ", tableName='" + tableName + '\'' +
                ", createTime='" + createTime + '\'' +
                ", comment='" + comment + '\'' +
                ", totalSize=" + totalSize +
                ", rows=" + rows +
                '}';
    }
}
