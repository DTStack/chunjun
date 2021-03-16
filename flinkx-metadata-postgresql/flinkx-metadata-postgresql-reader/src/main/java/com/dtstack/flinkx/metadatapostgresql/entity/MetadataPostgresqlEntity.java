package com.dtstack.flinkx.metadatapostgresql.entity;

import com.dtstack.metadata.rdb.core.entity.MetadatardbEntity;


/**
 * Company：www.dtstack.com
 *
 * @author shitou
 * @date 2021/1/27 17:14
 */
public class MetadataPostgresqlEntity extends MetadatardbEntity {


    /**数据库名*/
    private String dataBaseName;

    public String getDataBaseName() {
        return dataBaseName;
    }

    public void setDataBaseName(String dataBaseName) {
        this.dataBaseName = dataBaseName;
    }

    @Override
    public String toString() {
        return "MetadataPostgresqlEntity{" +
                "dataBaseName='" + dataBaseName + '\'' +
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
