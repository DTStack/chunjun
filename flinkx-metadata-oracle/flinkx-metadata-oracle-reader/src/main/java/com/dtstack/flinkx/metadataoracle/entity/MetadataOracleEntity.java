package com.dtstack.flinkx.metadataoracle.entity;

import com.dtstack.metadata.rdb.core.entity.MetadatardbEntity;

import java.util.List;

/**
 * @company:www.dtstack.com
 * @Author:shiFang
 * @Date:2021-01-28 14:34
 * @Description:
 */
public class MetadataOracleEntity extends MetadatardbEntity {

    List<OracleIndexEntity> oracleIndexEntityList;

    List<OracleColumnEntity> partitionColumns;

    public List<OracleIndexEntity> getOracleIndexEntityList() {
        return oracleIndexEntityList;
    }

    public void setOracleIndexEntityList(List<OracleIndexEntity> oracleIndexEntityList) {
        this.oracleIndexEntityList = oracleIndexEntityList;
    }

    public List<OracleColumnEntity> getPartitionColumns() {
        return partitionColumns;
    }

    public void setPartitionColumns(List<OracleColumnEntity> partitionColumns) {
        this.partitionColumns = partitionColumns;
    }
}
