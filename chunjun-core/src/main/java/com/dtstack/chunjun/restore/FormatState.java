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

package com.dtstack.chunjun.restore;

import org.apache.flink.api.common.accumulators.LongCounter;

import java.io.Serializable;
import java.util.Map;

public class FormatState implements Serializable {

    private static final long serialVersionUID = 1L;

    private int numOfSubTask;

    private Object state;

    /** store metric info */
    private Map<String, LongCounter> metric;

    private long numberRead;

    private long numberWrite;

    private String jobId;

    private int fileIndex = -1;

    public FormatState() {}

    public FormatState(int numOfSubTask, Object state) {
        this.numOfSubTask = numOfSubTask;
        this.state = state;
    }

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public int getFileIndex() {
        return fileIndex;
    }

    public void setFileIndex(int fileIndex) {
        this.fileIndex = fileIndex;
    }

    public long getNumberRead() {
        return numberRead;
    }

    public void setNumberRead(long numberRead) {
        this.numberRead = numberRead;
    }

    public long getNumberWrite() {
        return numberWrite;
    }

    public void setNumberWrite(long numberWrite) {
        this.numberWrite = numberWrite;
    }

    public int getNumOfSubTask() {
        return numOfSubTask;
    }

    public void setNumOfSubTask(int numOfSubTask) {
        this.numOfSubTask = numOfSubTask;
    }

    public Object getState() {
        return state;
    }

    public void setState(Object state) {
        this.state = state;
    }

    public Map<String, LongCounter> getMetric() {
        return metric;
    }

    public long getMetricValue(String key) {
        if (metric != null) {
            return metric.get(key).getLocalValue();
        }
        return 0;
    }

    public void setMetric(Map<String, LongCounter> metric) {
        this.metric = metric;
    }

    @Override
    public String toString() {
        return "FormatState{"
                + "numOfSubTask="
                + numOfSubTask
                + ", state="
                + state
                + ", metric="
                + metric
                + ", numberRead="
                + numberRead
                + ", numberWrite="
                + numberWrite
                + ", jobId='"
                + jobId
                + '\''
                + ", fileIndex="
                + fileIndex
                + '}';
    }
}
