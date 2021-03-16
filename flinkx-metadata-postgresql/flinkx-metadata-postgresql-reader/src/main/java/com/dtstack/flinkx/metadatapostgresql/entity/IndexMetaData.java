package com.dtstack.flinkx.metadatapostgresql.entity;


import java.io.Serializable;
import java.util.List;

/**
 * 定义索引的元数据
 *
 * @author shitou
 * @date 2020/12/29 13:52
 */
public class IndexMetaData implements Serializable {

    /**索引名*/
    private String indexName;

    /**索引列*/
    private List<String> columns;

    /**索引类型*/
    private String indexType;


    public IndexMetaData() {
    }

    public IndexMetaData(String indexName, List<String> columns, String indexType) {
        this.indexName = indexName;
        this.columns = columns;
        this.indexType = indexType;
    }

    public String getIndexName() {
        return indexName;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    public List<String> getColumns() {
        return columns;
    }

    public void setColumns(List<String> columns) {
        this.columns = columns;
    }

    public String getIndexType() {
        return indexType;
    }

    public void setIndexType(String indexType) {
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
