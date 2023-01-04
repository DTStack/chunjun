/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.dtstack.chunjun.connector.influxdb.source;

import org.apache.flink.core.io.GenericInputSplit;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class InfluxdbInputSplit extends GenericInputSplit {

    private static final long serialVersionUID = 6034593427931694722L;

    private int mod;

    /**
     * Creates a generic input split with the given split number.
     *
     * @param partitionNumber The number of the split's partition.
     * @param totalNumberOfPartitions The total number of the splits (partitions).
     */
    public InfluxdbInputSplit(int partitionNumber, int totalNumberOfPartitions, int mod) {
        super(partitionNumber, totalNumberOfPartitions);
        this.mod = mod;
    }
}
