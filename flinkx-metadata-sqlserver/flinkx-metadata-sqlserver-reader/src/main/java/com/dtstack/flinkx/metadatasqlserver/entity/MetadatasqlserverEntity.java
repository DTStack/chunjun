package com.dtstack.flinkx.metadatasqlserver.entity;

import com.dtstack.metadata.rdb.core.entity.ColumnEntity;
import com.dtstack.metadata.rdb.core.entity.MetadatardbEntity;

import java.util.List;

/**
 * Company：www.dtstack.com
 *
 * @author shitou
 * @date 2021/2/1 10:51
 */
public class MetadatasqlserverEntity extends MetadatardbEntity {

    /**
     * 数据库名
     */
    private String dataBaseName;

    /**
     * 分区字段
     */
    private List<ColumnEntity> partionColumn;

    public String getDataBaseName() {
        return dataBaseName;
    }

    public void setDataBaseName(String dataBaseName) {
        this.dataBaseName = dataBaseName;
    }

    public List<ColumnEntity> getPartionColumn() {
        return partionColumn;
    }

    public void setPartionColumn(List<ColumnEntity> partionColumn) {
        this.partionColumn = partionColumn;
    }

    @Override
    public String toString() {
        return "MetadatasqlserverEntity{" +
                "partionColumn=" + partionColumn +
                ", tableProperties=" + tableProperties +
                ", columns=" + columns +
                ", tableName='" + tableName + '\'' +
                ", schema='" + schema + '\'' +
                ", querySuccess=" + querySuccess +
                ", errorMsg='" + errorMsg + '\'' +
                ", operaType='" + operaType + '\'' +
                '}';
    }
}
