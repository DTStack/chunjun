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

package com.dtstack.chunjun.connector.hbase.source;

import org.apache.flink.core.io.InputSplit;

import lombok.AllArgsConstructor;
import lombok.Getter;

/** The Class describing each InputSplit of HBase */
@Getter
@AllArgsConstructor
public class HBaseInputSplit implements InputSplit {

    private static final long serialVersionUID = 5741383494402835110L;
    private final String startkey;

    private final String endKey;

    public int getSplitNumber() {
        return 0;
    }
}
