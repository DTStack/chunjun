package com.dtstack.flinkx.metadatapostgresql.pojo;


import java.util.ArrayList;

/**
 * flinkx-all com.dtstack.flinkx.metadatapostgresql.pojo
 *
 * @author shitou
 * @description //定义索引的元数据
 * @date 2020/12/29 13:52
 */
public class IndexMetaData {

    /**
     * 索引名
     */
    private String indexName;

    /**
     * 索引列名
     */
    private ArrayList<String> columns;

    /**
     * 索引类型
     */
    private String indexType;


    public IndexMetaData() {
    }

    public IndexMetaData(String indexName, ArrayList<String> columns, String indexType) {
        this.indexName = indexName;
        this.columns = columns;
        this.indexType = indexType;
    }

    @Override
    public String toString() {
        return "IndexMetaData{" +
                "indexName='" + indexName + '\'' +
                ", columns=" + columns +
                ", indexType='" + indexType + '\'' +
                '}';
    }
}
