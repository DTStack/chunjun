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
package com.dtstack.chunjun.connector.jdbc.source.distribute;

import com.dtstack.chunjun.connector.jdbc.config.DataSourceConfig;
import com.dtstack.chunjun.connector.jdbc.source.JdbcInputSplit;

import lombok.Getter;

import java.util.List;

@Getter
public class DistributedJdbcInputSplit extends JdbcInputSplit {

    private static final long serialVersionUID = 2553332495976709173L;

    private final List<DataSourceConfig> sourceList;

    public DistributedJdbcInputSplit(
            int partitionNumber,
            int totalNumberOfPartitions,
            List<DataSourceConfig> sourceList,
            String splitStrategy,
            boolean isPolling) {
        super(
                partitionNumber,
                totalNumberOfPartitions,
                partitionNumber,
                null,
                null,
                null,
                null,
                splitStrategy,
                isPolling);
        this.sourceList = sourceList;
    }
}
