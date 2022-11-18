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

class SettingConfigTest {

    private RestoreConfig restoreConfig = new RestoreConfig();
    private SettingConfig settingConfig = new SettingConfig();

    @BeforeEach
    public void setUp() {
        restoreConfig = new RestoreConfig();
        settingConfig = new SettingConfig();
    }

    @Test
    @DisplayName("Should set the restore when the restore is not null")
    public void setRestoreWhenRestoreIsNotNull() {
        settingConfig.setRestore(restoreConfig);
        assertEquals(restoreConfig, settingConfig.getRestore());
    }

    @Test
    @DisplayName("Should set the speed")
    public void setSpeedShouldSetTheSpeed() {
        SpeedConfig speedConfig = new SpeedConfig();
        speedConfig.setChannel(2);
        speedConfig.setReaderChannel(3);
        speedConfig.setWriterChannel(4);
        speedConfig.setBytes(5L);
        speedConfig.setRebalance(true);

        settingConfig.setSpeed(speedConfig);

        assertEquals(speedConfig, settingConfig.getSpeed());
    }

    @Test
    @DisplayName("Should set the metricpluginconf")
    public void setMetricPluginConfShouldSetTheMetricPluginConf() {
        MetricPluginConfig metricPluginConfig = new MetricPluginConfig();

        settingConfig.setMetricPluginConfig(metricPluginConfig);

        assertEquals(metricPluginConfig, settingConfig.getMetricPluginConfig());
    }
}
