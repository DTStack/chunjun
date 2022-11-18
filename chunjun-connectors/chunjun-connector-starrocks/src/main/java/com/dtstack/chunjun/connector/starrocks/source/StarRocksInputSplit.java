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

package com.dtstack.chunjun.connector.starrocks.source;

import com.dtstack.chunjun.connector.starrocks.source.be.entity.QueryBeXTablets;

import org.apache.flink.core.io.GenericInputSplit;

import lombok.Getter;

@Getter
public class StarRocksInputSplit extends GenericInputSplit {

    private static final long serialVersionUID = -8022476790996978880L;

    private final QueryBeXTablets queryBeXTablets;
    private final String opaquedQueryPlan;

    /**
     * Creates a generic input split with the given split number.
     *
     * @param partitionNumber The number of the split's partition.
     * @param totalNumberOfPartitions The total number of the splits (partitions).
     */
    public StarRocksInputSplit(
            int partitionNumber,
            int totalNumberOfPartitions,
            QueryBeXTablets queryBeXTablets,
            String opaquedQueryPlan) {
        super(partitionNumber, totalNumberOfPartitions);
        this.queryBeXTablets = queryBeXTablets;
        this.opaquedQueryPlan = opaquedQueryPlan;
    }
}
