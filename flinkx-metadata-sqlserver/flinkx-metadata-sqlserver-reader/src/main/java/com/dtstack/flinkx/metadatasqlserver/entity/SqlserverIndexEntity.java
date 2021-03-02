package com.dtstack.flinkx.metadatasqlserver.entity;

import java.io.Serializable;
import java.util.List;

/**
 * Company：www.dtstack.com
 *
 * @author shitou
 * @date 2021/2/1 15:17
 */
public class SqlserverIndexEntity implements Serializable {

    protected static final long serialVersionUID = 1L;

    /**索引名*/
    private String indexName;

    /**索引类型*/
    private String indexType;

    /**索引字段*/
    private List<String> indexColumn;

    public SqlserverIndexEntity() {
    }

    public SqlserverIndexEntity(String indexName, String indexType, List<String> indexColumn) {
        this.indexName = indexName;
        this.indexType = indexType;
        this.indexColumn = indexColumn;
    }

    public String getIndexName() {
        return indexName;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    public String getIndexType() {
        return indexType;
    }

    public void setIndexType(String indexType) {
        this.indexType = indexType;
    }

    public List<String> getIndexColumn() {
        return indexColumn;
    }

    public void setIndexColumn(List<String> indexColumn) {
        this.indexColumn = indexColumn;
    }

    @Override
    public String toString() {
        return "SqlserverIndexEntity{" +
                "indexName='" + indexName + '\'' +
                ", indexType='" + indexType + '\'' +
                ", indexColumn=" + indexColumn +
                '}';
    }
}
