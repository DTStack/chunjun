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

package com.dtstack.chunjun.dirty.utils;

import com.dtstack.chunjun.dirty.impl.DirtyDataEntry;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class LogUtilTest {
    private Logger log;

    @BeforeEach
    public void setUp() {
        log = LoggerFactory.getLogger(LogUtilTest.class);
    }

    @Test
    @DisplayName("Should log the message when the currentnumber is divisible by rate")
    public void warnWhenCurrentNumberIsDivisibleByRateThenLogMessage() {

        String message = "message";
        Throwable cause = null;
        long rate = 2;
        long currentNumber = 2;

        LogUtil.warn(log, message, cause, rate, currentNumber);
    }

    @Test
    @DisplayName("Should log the message and cause when the currentnumber is divisible by rate")
    public void warnWhenCurrentNumberIsDivisibleByRateThenLogMessageAndCause() {
        String message = "message";
        Throwable cause = new Throwable();
        long rate = 2;
        long currentNumber = 2;

        LogUtil.warn(log, message, cause, rate, currentNumber);
    }

    @Test
    @DisplayName("Should print the dirty data when the currentnumber is divisible by rate")
    public void printDirtyWhenCurrentNumberIsDivisibleByRate() {
        DirtyDataEntry dirty = new DirtyDataEntry();
        long rate = 10;
        long currentNumber = 10;

        LogUtil.printDirty(log, dirty, rate, currentNumber);
    }

    @Test
    @DisplayName("Should not print the dirty data when the currentnumber is not divisible by rate")
    public void printDirtyWhenCurrentNumberIsNotDivisibleByRate() {
        DirtyDataEntry dirty = new DirtyDataEntry();
        LogUtil.printDirty(log, dirty, 2, 1);
    }
}
