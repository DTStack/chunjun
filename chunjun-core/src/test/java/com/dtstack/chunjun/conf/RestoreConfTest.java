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

package com.dtstack.chunjun.conf;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RestoreConfTest {

    private RestoreConf restoreConf;

    @BeforeEach
    public void setUp() {
        restoreConf = new RestoreConf();
    }

    @Test
    @DisplayName("Should set the maxrownumforcheckpoint when the value is positive")
    public void setMaxRowNumForCheckpointWhenValueIsPositive() {
        restoreConf.setMaxRowNumForCheckpoint(10);
        assertEquals(10, restoreConf.getMaxRowNumForCheckpoint());
    }

    @Test
    @DisplayName("Should set the restorecolumntype when the input is not null")
    public void setRestoreColumnTypeWhenInputIsNotNull() {
        String input = "test";
        restoreConf.setRestoreColumnType(input);
        assertEquals(input, restoreConf.getRestoreColumnType());
    }

    @Test
    @DisplayName("Should set the restorecolumnname when the input is not null")
    public void setRestoreColumnNameWhenInputIsNotNull() {
        String restoreColumnName = "restoreColumnName";
        restoreConf.setRestoreColumnName(restoreColumnName);
        assertEquals(restoreColumnName, restoreConf.getRestoreColumnName());
    }

    @Test
    @DisplayName("Should set the restorecolumnindex to the given value")
    public void setRestoreColumnIndexShouldSetTheRestoreColumnIndexToTheGivenValue() {
        restoreConf.setRestoreColumnIndex(1);
        assertEquals(1, restoreConf.getRestoreColumnIndex());
    }

    @Test
    @DisplayName("Should set the restore to true")
    public void setRestoreWhenRestoreIsTrue() {
        restoreConf.setRestore(true);
        assertTrue(restoreConf.isRestore());
    }

    @Test
    @DisplayName("Should set the restore to false")
    public void setRestoreWhenRestoreIsFalse() {
        restoreConf.setRestore(false);
        assertFalse(restoreConf.isRestore());
    }

    @Test
    @DisplayName("Should set the isstream to true when the input is true")
    public void setStreamWhenInputIsTrueThenSetIsStreamToTrue() {
        restoreConf.setStream(true);
        assertTrue(restoreConf.isStream());
    }

    @Test
    @DisplayName("Should set the isstream to false when the input is false")
    public void setStreamWhenInputIsFalseThenSetIsStreamToFalse() {
        restoreConf.setStream(false);
        assertFalse(restoreConf.isStream());
    }

    @Test
    @DisplayName("Should return true when the restore is set to true")
    public void isRestoreWhenRestoreIsTrue() {
        restoreConf.setRestore(true);
        assertTrue(restoreConf.isRestore());
    }

    @Test
    @DisplayName("Should return false when the restore is set to false")
    public void isRestoreWhenRestoreIsFalse() {
        restoreConf.setRestore(false);
        assertFalse(restoreConf.isRestore());
    }

    @Test
    @DisplayName("Should return true when the stream is set to true")
    public void isStreamWhenStreamIsTrue() {
        restoreConf.setStream(true);
        assertTrue(restoreConf.isStream());
    }

    @Test
    @DisplayName("Should return false when the stream is set to false")
    public void isStreamWhenStreamIsFalse() {
        restoreConf.setStream(false);
        assertFalse(restoreConf.isStream());
    }
}
