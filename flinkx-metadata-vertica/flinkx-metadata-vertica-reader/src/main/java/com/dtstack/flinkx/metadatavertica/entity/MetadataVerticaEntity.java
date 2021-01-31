package com.dtstack.flinkx.metadatavertica.entity;

import com.dtstack.metadata.rdb.core.entity.ColumnEntity;
import com.dtstack.metadata.rdb.core.entity.MetadatardbEntity;

import java.util.List;

/**
 * @company:www.dtstack.com
 * @Author:shiFang
 * @Date:2021-01-28 19:25
 * @Description:
 */
public class MetadataVerticaEntity extends MetadatardbEntity {

    private List<ColumnEntity> partitionColumns;

    public List<ColumnEntity> getPartitionColumns() {
        return partitionColumns;
    }

    public void setPartitionColumns(List<ColumnEntity> partitionColumns) {
        this.partitionColumns = partitionColumns;
    }
}
