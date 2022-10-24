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

class SettingConfTest {

    private RestoreConf restoreConf = new RestoreConf();
    private SettingConf settingConf = new SettingConf();

    @BeforeEach
    public void setUp() {
        restoreConf = new RestoreConf();
        settingConf = new SettingConf();
    }

    @Test
    @DisplayName("Should set the log")
    public void setLogShouldSetTheLog() {
        LogConf logConf = new LogConf();
        logConf.setLogger(true);
        logConf.setLevel("info");
        logConf.setPath("/tmp/dtstack/chunjun/");
        logConf.setPattern("%d{yyyy-MM-dd HH:mm:ss} %-5p %c{1}:%L - %m%n");

        settingConf.setLog(logConf);

        assertEquals(logConf, settingConf.getLog());
    }

    @Test
    @DisplayName("Should set the restore when the restore is not null")
    public void setRestoreWhenRestoreIsNotNull() {
        settingConf.setRestore(restoreConf);
        assertEquals(restoreConf, settingConf.getRestore());
    }

    @Test
    @DisplayName("Should set the restart")
    public void setRestartShouldSetTheRestart() {
        RestartConf restartConf = new RestartConf();
        restartConf.setStrategy("no_restart");
        restartConf.setRestartAttempts(3);
        restartConf.setDelayInterval(10);
        restartConf.setFailureRate(2);
        restartConf.setFailureInterval(60);

        settingConf.setRestart(restartConf);

        assertEquals(restartConf, settingConf.getRestart());
    }

    @Test
    @DisplayName("Should set the speed")
    public void setSpeedShouldSetTheSpeed() {
        SpeedConf speedConf = new SpeedConf();
        speedConf.setChannel(2);
        speedConf.setReaderChannel(3);
        speedConf.setWriterChannel(4);
        speedConf.setBytes(5L);
        speedConf.setRebalance(true);

        settingConf.setSpeed(speedConf);

        assertEquals(speedConf, settingConf.getSpeed());
    }

    @Test
    @DisplayName("Should set the metricpluginconf")
    public void setMetricPluginConfShouldSetTheMetricPluginConf() {
        MetricPluginConf metricPluginConf = new MetricPluginConf();

        settingConf.setMetricPluginConf(metricPluginConf);

        assertEquals(metricPluginConf, settingConf.getMetricPluginConf());
    }
}
