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

    /**数据库名*/
    private String databaseName;

    /**分区字段*/
    private List<ColumnEntity> partionColumns;

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public List<ColumnEntity> getPartionColumns() {
        return partionColumns;
    }

    public void setPartionColumns(List<ColumnEntity> partionColumns) {
        this.partionColumns = partionColumns;
    }

    @Override
    public String toString() {
        return "MetadatasqlserverEntity{" +
                "partionColumn=" + partionColumns +
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
