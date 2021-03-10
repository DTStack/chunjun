/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.flinkx.metatdata.hive.core.entity;

import com.dtstack.metadata.rdb.core.entity.ColumnEntity;
import com.dtstack.metadata.rdb.core.entity.MetadatardbEntity;

import java.util.List;

/**
 * @company:www.dtstack.com
 * @Author:shiFang
 * @Date:2021-01-20 11:01
 * @Description:
 */
public class MetadataHiveEntity extends MetadatardbEntity {

    /**分区集合*/
    private List<String> partitions;

    /**分区字段信息*/
    private List<ColumnEntity> partitionColumns;

    public List<String> getPartitions() {
        return partitions;
    }

    public void setPartitions(List<String> partitions) {
        this.partitions = partitions;
    }

    public List<ColumnEntity> getPartitionColumns() {
        return partitionColumns;
    }

    public void setPartitionColumns(List<ColumnEntity> partitionColumns) {
        this.partitionColumns = partitionColumns;
    }

    @Override
    public String toString() {
        return "MetadataHiveEntity{" +
                "partitions=" + partitions +
                ", partitionColumns=" + partitionColumns +
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
