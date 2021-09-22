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

package com.dtstack.flinkx.dirty.impl;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.StringJoiner;

/**
 * @author tiezhu@dtstack
 * @date 2021/9/22 星期三
 */
public class DirtyDataEntry implements Serializable {

    private static final long serialVersionUID = 1L;

    private String jobId;

    private String jobName;

    private String operatorName;

    private String dirtyContent;

    private String errorMessage;

    private String fieldName;

    private Timestamp createTime;

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public String getOperatorName() {
        return operatorName;
    }

    public void setOperatorName(String operatorName) {
        this.operatorName = operatorName;
    }

    public String getDirtyContent() {
        return dirtyContent;
    }

    public void setDirtyContent(String dirtyContent) {
        this.dirtyContent = dirtyContent;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public Timestamp getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Timestamp createTime) {
        this.createTime = createTime;
    }

    /**
     * Transform dirty data to String arrays.
     *
     * @return string array of dirty data.
     */
    public String[] toArray() {
        return new String[] {
            jobId,
            jobName,
            operatorName,
            dirtyContent,
            errorMessage,
            fieldName,
            String.valueOf(createTime)
        };
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", DirtyDataEntry.class.getSimpleName() + "[", "]")
                .add("jobId='" + jobId + "'")
                .add("jobName='" + jobName + "'")
                .add("operatorName='" + operatorName + "'")
                .add("dirtyContent='" + dirtyContent + "'")
                .add("errorMessage='" + errorMessage + "'")
                .add("fieldName='" + fieldName + "'")
                .add("createTime=" + createTime)
                .toString();
    }
}
