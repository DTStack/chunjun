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

    /**字段集合*/
    private List<MysqlColumnEntity> partitionColumnEntities;

    /**分区字段集合*/
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
