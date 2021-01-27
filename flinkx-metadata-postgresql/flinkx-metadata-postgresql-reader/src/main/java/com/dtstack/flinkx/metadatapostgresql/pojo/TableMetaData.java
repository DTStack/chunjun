package com.dtstack.flinkx.metadatapostgresql.pojo;


import java.util.ArrayList;
import java.util.HashMap;
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
    private ArrayList<String> primaryKey;

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

    /**
     * 表中的索引
     */
    private ArrayList<IndexMetaData> indexes;


    public TableMetaData() {

    }

    public TableMetaData(String tableName, ArrayList<String> primaryKey, Integer dataCount, String tableSize, LinkedList<ColumnMetaData> columns, ArrayList<IndexMetaData> indexes) {
        this.tableName = tableName;
        this.primaryKey = primaryKey;
        this.dataCount = dataCount;
        this.tableSize = tableSize;
        this.columns = columns;
        this.indexes = indexes;
    }

    @Override
    public String toString() {
        return "TableMetaData{" +
                "tableName='" + tableName + '\'' +
                ", primaryKey=" + primaryKey +
                ", dataCount=" + dataCount +
                ", tableSize='" + tableSize + '\'' +
                ", columns=" + columns +
                ", indexes=" + indexes +
                '}';
    }
}
