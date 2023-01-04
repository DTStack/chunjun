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

package com.dtstack.chunjun.connector.redis.inputsplit;

import org.apache.flink.core.io.GenericInputSplit;

import java.util.List;

public class RedisInputSplit extends GenericInputSplit {

    private static final long serialVersionUID = 5305567367658268144L;

    private List<String> key;

    public RedisInputSplit(int partitionNumber, int totalNumberOfPartitions, List<String> key) {
        super(partitionNumber, totalNumberOfPartitions);
        this.key = key;
    }

    public List<String> getKey() {
        return key;
    }

    public void setKey(List<String> key) {
        this.key = key;
    }
}
