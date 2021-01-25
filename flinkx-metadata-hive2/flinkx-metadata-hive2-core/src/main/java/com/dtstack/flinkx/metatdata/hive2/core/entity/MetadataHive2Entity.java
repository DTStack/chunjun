package com.dtstack.flinkx.metatdata.hive2.core.entity;

import com.dtstack.metadata.rdb.core.entity.MetadatardbEntity;

import java.util.List;

/**
 * @company:www.dtstack.com
 * @Author:shiFang
 * @Date:2021-01-20 11:01
 * @Description:
 */
public class MetadataHive2Entity extends MetadatardbEntity {

    private List<String> partitions;

    private List<HiveColumnEntity> partitionColumns;

    public List<String> getPartitions() {
        return partitions;
    }

    public void setPartitions(List<String> partitions) {
        this.partitions = partitions;
    }

    public List<HiveColumnEntity> getPartitionColumns() {
        return partitionColumns;
    }

    public void setPartitionColumns(List<HiveColumnEntity> partitionColumns) {
        this.partitionColumns = partitionColumns;
    }

}
