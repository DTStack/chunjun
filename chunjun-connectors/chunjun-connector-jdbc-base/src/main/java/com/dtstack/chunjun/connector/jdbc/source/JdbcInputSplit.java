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

package com.dtstack.chunjun.connector.jdbc.source;

import org.apache.flink.core.io.GenericInputSplit;

/**
 * @author jiangbo
 * @explanation
 * @date 2019/3/6
 */
public class JdbcInputSplit extends GenericInputSplit {

    private int mod;

    private String endLocation;

    private String startLocation;

    /** 分片startLocation * */
    private String startLocationOfSplit;

    /** 分片endLocation * */
    private String endLocationOfSplit;

    /**
     * Creates a generic input split with the given split number.
     *
     * @param partitionNumber The number of the split's partition.
     * @param totalNumberOfPartitions The total number of the splits (partitions).
     */
    public JdbcInputSplit(
            int partitionNumber,
            int totalNumberOfPartitions,
            int mod,
            String startLocation,
            String endLocation,
            String startLocationOfSplit,
            String endLocationOfSplit) {
        super(partitionNumber, totalNumberOfPartitions);
        this.mod = mod;
        this.startLocation = startLocation;
        this.endLocation = endLocation;
        this.startLocationOfSplit = startLocationOfSplit;
        this.endLocationOfSplit = endLocationOfSplit;
    }

    public int getMod() {
        return mod;
    }

    public void setMod(int mod) {
        this.mod = mod;
    }

    public String getEndLocation() {
        return endLocation;
    }

    public void setEndLocation(String endLocation) {
        this.endLocation = endLocation;
    }

    public String getStartLocation() {
        return startLocation;
    }

    public void setStartLocation(String startLocation) {
        this.startLocation = startLocation;
    }

    public String getStartLocationOfSplit() {
        return startLocationOfSplit;
    }

    public void setStartLocationOfSplit(String startLocationOfSplit) {
        this.startLocationOfSplit = startLocationOfSplit;
    }

    public String getEndLocationOfSplit() {
        return endLocationOfSplit;
    }

    public void setEndLocationOfSplit(String endLocationOfSplit) {
        this.endLocationOfSplit = endLocationOfSplit;
    }

    @Override
    public String toString() {
        return "JdbcInputSplit{"
                + "mod="
                + mod
                + ", endLocation='"
                + endLocation
                + '\''
                + ", startLocation='"
                + startLocation
                + '\''
                + ", startLocationOfSplit='"
                + startLocationOfSplit
                + '\''
                + ", endLocationOfSplit='"
                + endLocationOfSplit
                + '\''
                + '}';
    }
}
