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

    /**oracle 索引集合*/
    List<OracleIndexEntity> oracleIndexEntityList;

    /**oracle 分区字段集合*/
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

    @Override
    public String toString() {
        return "MetadataOracleEntity{" +
                "oracleIndexEntityList=" + oracleIndexEntityList +
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
