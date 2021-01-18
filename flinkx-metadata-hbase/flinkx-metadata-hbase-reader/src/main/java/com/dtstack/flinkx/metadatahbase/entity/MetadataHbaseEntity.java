package com.dtstack.flinkx.metadatahbase.entity;

import com.dtstack.flinkx.metadata.entity.MetadataEntity;

import java.util.List;

/**
 * @company:www.dtstack.com
 * @Author:shiFang
 * @Date:2021-01-18 19:16
 * @Description:
 */
public class MetadataHbaseEntity extends MetadataEntity {

    private HbaseTableEntity tableProperties;

    private List<HbaseColumnEntity> columns;

    private String tableName;

    public void setTableProperties(HbaseTableEntity tableProperties) {
        this.tableProperties = tableProperties;
    }

    public void setColumns(List<HbaseColumnEntity> columns) {
        this.columns = columns;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
}
