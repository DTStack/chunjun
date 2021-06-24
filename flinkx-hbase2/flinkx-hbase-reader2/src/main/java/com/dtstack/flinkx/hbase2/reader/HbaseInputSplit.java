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

package com.dtstack.flinkx.hbase2.reader;

import org.apache.flink.core.io.InputSplit;

/**
 * The Class describing each InputSplit of HBase
 *
 * Company: cmss
 * @author wangyulei_yewu@cmss.chinamobile.com
 */
public class HbaseInputSplit implements InputSplit {

    private String startkey;
    private String endKey;

    public HbaseInputSplit(String startKey, String endKey) {
        this.startkey = startKey;
        this.endKey = endKey;
    }

    public String getStartkey() {
        return startkey;
    }

    public String getEndKey() {
        return endKey;
    }

    @Override
    public int getSplitNumber() {
        return 0;
    }
}
