package com.dtstack.flinkx.metadatapostgresql.pojo;

import java.util.LinkedList;

/**
 * flinkx-all com.dtstack.flinkx.metadatapostgresql.pojo
 *
 * @author shitou
 * @description //表的元数据
 * @date 2020/12/10 15:25
 */
public class TableMetaData {

    /**
     * 表名
     */
    private String tableName;
    /**
     * 主键
     */
    private String primaryKeyName;

    /**
     * 表中有多条数据
     */
    private Integer dataCount;

    /**
     * 表所占磁盘空间大小
     */
    private String tableSize;

    /**
     * 表中的所有字段
     */
    private LinkedList<ColumnMetaData> columns;


    public TableMetaData() {

    }

    public TableMetaData(String tableName, String primaryKeyName, Integer dataCount, String tableSize, LinkedList<ColumnMetaData> columns) {
        this.tableName = tableName;
        this.primaryKeyName = primaryKeyName;
        this.dataCount = dataCount;
        this.tableSize = tableSize;
        this.columns = columns;
    }

    @Override
    public String toString() {
        return "TableMetaData{" +
                "tableName='" + tableName + '\'' +
                ", primaryKeyName='" + primaryKeyName + '\'' +
                ", dataCount=" + dataCount +
                ", tableSize='" + tableSize + '\'' +
                ", columns=" + columns +
                '}';
    }
}
