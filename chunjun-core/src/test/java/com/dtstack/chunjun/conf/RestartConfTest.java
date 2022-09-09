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

class RestartConfTest {
    private RestartConf restartConf;

    @BeforeEach
    public void setUp() {
        restartConf = new RestartConf();
    }

    @Test
    @DisplayName("Should set the restartattempts when the value is greater than 0")
    public void setRestartAttemptsWhenValueIsGreaterThanZero() {
        restartConf.setRestartAttempts(1);
        assertEquals(1, restartConf.getRestartAttempts());
    }

    @Test
    @DisplayName("Should set the failureinterval")
    public void setFailureIntervalShouldSetTheFailureInterval() {
        RestartConf restartConf = new RestartConf();
        restartConf.setFailureInterval(60);
        assertEquals(60, restartConf.getFailureInterval());
    }

    @Test
    @DisplayName("Should set the strategy when the strategy is valid")
    public void setStrategyWhenStrategyIsValid() {
        RestartConf restartConf = new RestartConf();
        restartConf.setStrategy("NoRestart");
        assertEquals("NoRestart", restartConf.getStrategy());
    }

    @Test
    @DisplayName("Should set the failure rate")
    public void setFailureRate() {
        RestartConf restartConf = new RestartConf();
        restartConf.setFailureRate(2);
        assertEquals(2, restartConf.getFailureRate());
    }

    @Test
    @DisplayName("Should set the delayinterval")
    public void setDelayIntervalShouldSetTheDelayInterval() {
        RestartConf restartConf = new RestartConf();
        restartConf.setDelayInterval(10);
        assertEquals(10, restartConf.getDelayInterval());
    }
}
