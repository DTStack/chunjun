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

package com.dtstack.flinkx.odps.reader;

import org.apache.flink.core.io.InputSplit;

/**
 * The Class describing each Odps InputSplit
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class OdpsInputSplit implements InputSplit {

    private String sessionId;
    private long startIndex;
    private long stepCount;

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    public long getStartIndex() {
        return startIndex;
    }

    public void setStartIndex(long startIndex) {
        this.startIndex = startIndex;
    }

    public long getStepCount() {
        return stepCount;
    }

    public void setStepCount(long stepCount) {
        this.stepCount = stepCount;
    }

    public OdpsInputSplit(String sessionId, long startIndex, long stepCount) {
        this.sessionId = sessionId;
        this.startIndex = startIndex;
        this.stepCount = stepCount;
    }

    @Override
    public int getSplitNumber() {
        return 0;
    }
}
