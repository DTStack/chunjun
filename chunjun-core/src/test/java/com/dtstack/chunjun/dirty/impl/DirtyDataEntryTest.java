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

package com.dtstack.chunjun.dirty.impl;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Timestamp;
import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class DirtyDataEntryTest {

    @Test
    @DisplayName(
            "Should return an array of strings with the same length as the number of fields in dirtydataentry")
    void toArrayShouldReturnAnArrayOfStringsWithTheSameLengthAsTheNumberOfFieldsInDirtyDataEntry() {
        DirtyDataEntry dirtyDataEntry = new DirtyDataEntry();
        dirtyDataEntry.setJobId("jobId");
        dirtyDataEntry.setJobName("jobName");
        dirtyDataEntry.setOperatorName("operatorName");
        dirtyDataEntry.setDirtyContent("dirtyContent");
        dirtyDataEntry.setErrorMessage("errorMessage");
        dirtyDataEntry.setFieldName("fieldName");
        dirtyDataEntry.setCreateTime(Timestamp.valueOf(LocalDateTime.now()));

        String[] result = dirtyDataEntry.toArray();

        assertEquals(7, result.length);
    }

    @Test
    @DisplayName(
            "Should return an array of strings with the same content as the fields in dirtydataentry")
    void toArrayShouldReturnAnArrayOfStringsWithTheSameContentAsTheFieldsInDirtyDataEntry() {
        DirtyDataEntry dirtyDataEntry = new DirtyDataEntry();
        dirtyDataEntry.setJobId("jobId");
        dirtyDataEntry.setJobName("jobName");
        dirtyDataEntry.setOperatorName("operatorName");
        dirtyDataEntry.setDirtyContent("dirtyContent");
        dirtyDataEntry.setErrorMessage("errorMessage");
        dirtyDataEntry.setFieldName("fieldName");

        Timestamp current = Timestamp.valueOf(LocalDateTime.now());

        dirtyDataEntry.setCreateTime(current);

        String[] expected =
                new String[] {
                    "jobId",
                    "jobName",
                    "operatorName",
                    "dirtyContent",
                    "errorMessage",
                    "fieldName",
                    String.valueOf(current)
                };

        assertArrayEquals(expected, dirtyDataEntry.toArray());
    }

    @Test
    @DisplayName("Should return a string with all fields")
    void toStringShouldReturnAStringWithAllFields() {
        DirtyDataEntry dirtyDataEntry = new DirtyDataEntry();
        dirtyDataEntry.setJobId("jobId");
        dirtyDataEntry.setJobName("jobName");
        dirtyDataEntry.setOperatorName("operatorName");
        dirtyDataEntry.setDirtyContent("dirtyContent");
        dirtyDataEntry.setErrorMessage("errorMessage");
        dirtyDataEntry.setFieldName("fieldName");
        dirtyDataEntry.setCreateTime(Timestamp.valueOf(LocalDateTime.now()));

        String expected =
                "DirtyDataEntry[jobId='jobId', jobName='jobName', operatorName='operatorName', "
                        + "dirtyContent='dirtyContent', errorMessage='errorMessage', fieldName='fieldName', "
                        + "createTime="
                        + dirtyDataEntry.getCreateTime()
                        + "]";

        assertEquals(expected, dirtyDataEntry.toString());
    }

    @Test
    @DisplayName("Should set the createtime to the current time")
    void setCreateTimeShouldSetTheCreateTimeToTheCurrentTime() {
        DirtyDataEntry dirtyDataEntry = new DirtyDataEntry();
        Timestamp currentTime = new Timestamp(System.currentTimeMillis());
        dirtyDataEntry.setCreateTime(currentTime);
        assertEquals(currentTime, dirtyDataEntry.getCreateTime());
    }

    @Test
    @DisplayName("Should set the fieldname")
    void setFieldNameShouldSetTheFieldName() {
        DirtyDataEntry dirtyDataEntry = new DirtyDataEntry();
        dirtyDataEntry.setFieldName("fieldName");
        assertEquals("fieldName", dirtyDataEntry.getFieldName());
    }

    @Test
    @DisplayName("Should set the error message")
    void setErrorMessageShouldSetTheErrorMessage() {
        DirtyDataEntry dirtyDataEntry = new DirtyDataEntry();
        dirtyDataEntry.setErrorMessage("error message");
        assertEquals("error message", dirtyDataEntry.getErrorMessage());
    }

    @Test
    @DisplayName("Should set the dirtycontent")
    void setDirtyContentShouldSetTheDirtyContent() {
        DirtyDataEntry dirtyDataEntry = new DirtyDataEntry();
        dirtyDataEntry.setDirtyContent("dirtyContent");
        assertEquals("dirtyContent", dirtyDataEntry.getDirtyContent());
    }

    @Test
    @DisplayName("Should set the operatorname")
    void setOperatorNameShouldSetTheOperatorName() {
        DirtyDataEntry dirtyDataEntry = new DirtyDataEntry();
        dirtyDataEntry.setOperatorName("test");
        assertEquals("test", dirtyDataEntry.getOperatorName());
    }

    @Test
    @DisplayName("Should set the jobname")
    void setJobNameShouldSetTheJobName() {
        DirtyDataEntry dirtyDataEntry = new DirtyDataEntry();
        dirtyDataEntry.setJobName("jobname");
        assertEquals("jobname", dirtyDataEntry.getJobName());
    }

    @Test
    @DisplayName("Should set the jobid")
    void setJobIdShouldSetTheJobId() {
        DirtyDataEntry dirtyDataEntry = new DirtyDataEntry();
        dirtyDataEntry.setJobId("jobId");
        assertEquals("jobId", dirtyDataEntry.getJobId());
    }
}
