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
     * 表中的所有字段
     */
    private LinkedList<ColumnMetaData> columns;

    /**
     * 主键
     */
    private String primaryKeyName;

    /**
     * 表中有多条数据
     */
    private Integer dataCount;

    public TableMetaData() {

    }

    public TableMetaData(String tableName, LinkedList<ColumnMetaData> columns, String primaryKeyName, Integer dataCount) {
        this.tableName = tableName;
        this.columns = columns;
        this.primaryKeyName = primaryKeyName;
        this.dataCount = dataCount;
    }

    @Override
    public String toString() {
        return "TableMetaData{" +
                "TableName='" + tableName + '\'' +
                ", columns=" + columns +
                ", primaryKeyName='" + primaryKeyName + '\'' +
                ", dataCount=" + dataCount +
                '}';
    }
}
