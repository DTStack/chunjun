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

package com.dtstack.chunjun.config;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RestoreConfigTest {

    private RestoreConfig restoreConfig;

    @BeforeEach
    public void setUp() {
        restoreConfig = new RestoreConfig();
    }

    @Test
    @DisplayName("Should set the maxrownumforcheckpoint when the value is positive")
    public void setMaxRowNumForCheckpointWhenValueIsPositive() {
        restoreConfig.setMaxRowNumForCheckpoint(10);
        assertEquals(10, restoreConfig.getMaxRowNumForCheckpoint());
    }

    @Test
    @DisplayName("Should set the restorecolumntype when the input is not null")
    public void setRestoreColumnTypeWhenInputIsNotNull() {
        String input = "test";
        restoreConfig.setRestoreColumnType(input);
        assertEquals(input, restoreConfig.getRestoreColumnType());
    }

    @Test
    @DisplayName("Should set the restorecolumnname when the input is not null")
    public void setRestoreColumnNameWhenInputIsNotNull() {
        String restoreColumnName = "restoreColumnName";
        restoreConfig.setRestoreColumnName(restoreColumnName);
        assertEquals(restoreColumnName, restoreConfig.getRestoreColumnName());
    }

    @Test
    @DisplayName("Should set the restorecolumnindex to the given value")
    public void setRestoreColumnIndexShouldSetTheRestoreColumnIndexToTheGivenValue() {
        restoreConfig.setRestoreColumnIndex(1);
        assertEquals(1, restoreConfig.getRestoreColumnIndex());
    }

    @Test
    @DisplayName("Should set the restore to true")
    public void setRestoreWhenRestoreIsTrue() {
        restoreConfig.setRestore(true);
        assertTrue(restoreConfig.isRestore());
    }

    @Test
    @DisplayName("Should set the restore to false")
    public void setRestoreWhenRestoreIsFalse() {
        restoreConfig.setRestore(false);
        assertFalse(restoreConfig.isRestore());
    }

    @Test
    @DisplayName("Should set the isstream to true when the input is true")
    public void setStreamWhenInputIsTrueThenSetIsStreamToTrue() {
        restoreConfig.setStream(true);
        assertTrue(restoreConfig.isStream());
    }

    @Test
    @DisplayName("Should set the isstream to false when the input is false")
    public void setStreamWhenInputIsFalseThenSetIsStreamToFalse() {
        restoreConfig.setStream(false);
        assertFalse(restoreConfig.isStream());
    }

    @Test
    @DisplayName("Should return true when the restore is set to true")
    public void isRestoreWhenRestoreIsTrue() {
        restoreConfig.setRestore(true);
        assertTrue(restoreConfig.isRestore());
    }

    @Test
    @DisplayName("Should return false when the restore is set to false")
    public void isRestoreWhenRestoreIsFalse() {
        restoreConfig.setRestore(false);
        assertFalse(restoreConfig.isRestore());
    }

    @Test
    @DisplayName("Should return true when the stream is set to true")
    public void isStreamWhenStreamIsTrue() {
        restoreConfig.setStream(true);
        assertTrue(restoreConfig.isStream());
    }

    @Test
    @DisplayName("Should return false when the stream is set to false")
    public void isStreamWhenStreamIsFalse() {
        restoreConfig.setStream(false);
        assertFalse(restoreConfig.isStream());
    }
}
