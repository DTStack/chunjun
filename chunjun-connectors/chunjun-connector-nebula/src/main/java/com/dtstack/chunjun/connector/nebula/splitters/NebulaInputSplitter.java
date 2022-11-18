/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.chunjun.connector.nebula.splitters;

import org.apache.flink.core.io.GenericInputSplit;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.LinkedList;

@Getter
@Setter
@ToString
public class NebulaInputSplitter extends GenericInputSplit {

    private static final long serialVersionUID = 5783977865973344109L;

    public LinkedList<Integer> parts = new LinkedList<>();

    /**
     * Pull data within a given time, and set the fetch-interval to achieve the effect of breakpoint
     * resuming, which is equivalent to dividing the time into multiple pull-up data according to
     * the fetch-interval
     */
    private Long interval;
    /** scan the data after the start-time insert */
    private Long scanStart;
    /** scan the data before the end-time insert */
    private Long scanEnd;

    public NebulaInputSplitter(
            Integer partitionNumber,
            Integer totalNumberOfPartitions,
            Long scanStart,
            Long scanEnd,
            Long interval) {
        super(partitionNumber, totalNumberOfPartitions);
        this.scanStart = scanStart;
        this.scanEnd = scanEnd;
        this.interval = interval;
    }
}
