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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SyncConfigTest {

    @Test
    @DisplayName("Should return the correct string when all fields are not null")
    public void toStringWhenAllFieldsAreNotNull() {
        SyncConfig syncConfig = new SyncConfig();
        syncConfig.setPluginRoot("pluginRoot");
        syncConfig.setRemotePluginPath("remotePluginPath");
        syncConfig.setSavePointPath("savePointPath");
        syncConfig.setSyncJarList(Collections.singletonList("syncJarList"));

        String expected =
                "SyncConfig[job=null, pluginRoot='pluginRoot', remotePluginPath='remotePluginPath', savePointPath='savePointPath', syncJarList=[syncJarList]]";
        assertEquals(expected, syncConfig.toString());
    }

    @Test
    @DisplayName("Should return the correct string when pluginRoot is null")
    public void toStringWhenPluginRootIsNull() {
        SyncConfig syncConfig = new SyncConfig();
        syncConfig.setPluginRoot(null);
        syncConfig.setRemotePluginPath("remotePluginPath");
        syncConfig.setSavePointPath("savePointPath");
        syncConfig.setSyncJarList(null);
    }

    @Test
    @DisplayName("Should set job")
    public void getOldDirtyConfWhenJobIsNullThenReturnNull() {
        SyncConfig syncConfig = new SyncConfig();
        syncConfig.setJob(null);
    }

    @Test
    @DisplayName("Should return the metricPluginConf when the metricPluginConf is not null")
    public void getMetricPluginConfWhenMetricPluginConfIsNotNull() {
        SyncConfig syncConfig = new SyncConfig();
        JobConfig jobConfig = new JobConfig();
        SettingConfig settingConfig = new SettingConfig();
        MetricPluginConfig metricPluginConfig = new MetricPluginConfig();
        settingConfig.setMetricPluginConfig(metricPluginConfig);
        jobConfig.setSetting(settingConfig);
        syncConfig.setJob(jobConfig);

        MetricPluginConfig result = syncConfig.getMetricPluginConf();

        assertNotNull(result);
    }

    @Test
    @DisplayName("Should set the savepointPath when the savepointPath is not null")
    public void setSavePointPathWhenSavePointPathIsNotNull() {
        SyncConfig syncConfig = new SyncConfig();
        syncConfig.setSavePointPath("/tmp/savepoint");
        assertNotNull(syncConfig.getSavePointPath());
    }

    @Test
    @DisplayName("Should set the pluginRoot when the pluginRoot is not null")
    public void setPluginRootWhenPluginRootIsNotNull() {
        SyncConfig syncConfig = new SyncConfig();
        syncConfig.setPluginRoot("/tmp/plugin");
        assertNotNull(syncConfig.getPluginRoot());
    }

    @Test
    @DisplayName("Should return the speed when the speed is not null")
    public void getSpeedWhenSpeedIsNotNull() {
        SyncConfig syncConfig = new SyncConfig();
        JobConfig jobConfig = new JobConfig();
        SettingConfig settingConfig = new SettingConfig();
        SpeedConfig speedConfig = new SpeedConfig();
        speedConfig.setChannel(1);
        settingConfig.setSpeed(speedConfig);
        jobConfig.setSetting(settingConfig);
        syncConfig.setJob(jobConfig);

        SpeedConfig speed = syncConfig.getSpeed();

        assertNotNull(speed);
    }

    @Test
    @DisplayName("Should throw an exception when the reader is null")
    void getReaderWhenReaderIsNullThenThrowException() {
        SyncConfig syncConfig = new SyncConfig();
        syncConfig.setJob(new JobConfig());

        assertThrows(NullPointerException.class, syncConfig::getReader);
    }

    @Test
    @DisplayName("Should throw an exception when content is empty")
    void checkJobWhenContentIsEmptyThenThrowException() {
        String jobJson = "{\"job\":{\"content\":[]}}";

        Throwable exception =
                assertThrows(IllegalArgumentException.class, () -> SyncConfig.parseJob(jobJson));

        assertEquals(
                "[content] in the task script is empty, please check the task script configuration.",
                exception.getMessage());
    }
}
