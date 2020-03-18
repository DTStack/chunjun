/**
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
package com.dtstack.flinkx.metadata.reader.inputformat;

import org.apache.flink.core.io.GenericInputSplit;

/**
 * @author : tiezhu
 * @date : 2020/3/8
 * @description :
 */
public class MetaDataInputSplit extends GenericInputSplit {
    private String dbUrl;
    private String tableName;
    private String dbName;


    /**
     * Creates a generic input split with the given split number.
     *
     * @param partitionNumber         The number of the split's partition.
     * @param totalNumberOfPartitions The total number of the splits (partitions).
     */
    public MetaDataInputSplit(int partitionNumber, int totalNumberOfPartitions, String dbUrl, String tablesName, String dbName) {
        super(partitionNumber, totalNumberOfPartitions);
        this.dbUrl = dbUrl;
        this.tableName = tablesName;
        this.dbName = dbName;
    }

    public String getDbUrl() {
        return dbUrl;
    }

    public String getTableName() {
        return tableName;
    }

    public String getDbName() {
        return dbName;
    }

    @Override
    public String toString() {
        return "MetaDataInputSplit{" +
                "dbUrl='" + dbUrl + '\'' +
                ", tableName='" + tableName + '\'' +
                ", dbName='" + dbName + '\'' +
                '}';
    }
}
