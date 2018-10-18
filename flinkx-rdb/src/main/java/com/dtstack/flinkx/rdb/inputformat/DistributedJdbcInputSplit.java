/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.flinkx.rdb.inputformat;

import com.dtstack.flinkx.rdb.DataSource;
import org.apache.flink.core.io.GenericInputSplit;

import java.util.ArrayList;
import java.util.List;

/**
 * The split of DistributedJdbcInputFormat
 *
 * @Company: www.dtstack.com
 * @author jiangbo
 */
public class DistributedJdbcInputSplit extends GenericInputSplit {

    private List<DataSource> sourceList;

    public DistributedJdbcInputSplit(int partitionNumber, int totalNumberOfPartitions) {
        super(partitionNumber, totalNumberOfPartitions);
    }

    public void addSource(List<DataSource> sourceLeft){
        if (sourceList == null){
            this.sourceList = new ArrayList<>();
        }

        this.sourceList.addAll(sourceLeft);
    }

    public void addSource(DataSource source){
        if (sourceList == null){
            this.sourceList = new ArrayList<>();
        }

        this.sourceList.add(source);
    }

    public List<DataSource> getSourceList() {
        return sourceList;
    }

    public void setSourceList(List<DataSource> sourceList) {
        this.sourceList = sourceList;
    }
}
