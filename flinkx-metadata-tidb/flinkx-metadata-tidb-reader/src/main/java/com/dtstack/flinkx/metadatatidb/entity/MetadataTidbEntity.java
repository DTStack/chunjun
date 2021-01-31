package com.dtstack.flinkx.metadatatidb.entity;

import com.dtstack.flinkx.metadatamysql.entity.MetadataMysqlEntity;
import com.dtstack.flinkx.metadatamysql.entity.MysqlColumnEntity;

import java.util.List;

/**
 * @company:www.dtstack.com
 * @Author:shiFang
 * @Date:2021-01-29 11:19
 * @Description:
 */
public class MetadataTidbEntity extends MetadataMysqlEntity {

    private List<MysqlColumnEntity> partitionColumnEntities;

    private List<TidbPartitionEntity> partitions;

    public List<MysqlColumnEntity> getPartitionColumnEntities() {
        return partitionColumnEntities;
    }

    public void setPartitionColumnEntities(List<MysqlColumnEntity> partitionColumnEntities) {
        this.partitionColumnEntities = partitionColumnEntities;
    }

    public List<TidbPartitionEntity> getPartitions() {
        return partitions;
    }

    public void setPartitions(List<TidbPartitionEntity> partitions) {
        this.partitions = partitions;
    }
}
