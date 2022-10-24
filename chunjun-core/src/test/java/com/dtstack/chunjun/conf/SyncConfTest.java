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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SyncConfTest {

    @Test
    @DisplayName("Should return the correct string when all fields are not null")
    public void toStringWhenAllFieldsAreNotNull() {
        SyncConf syncConf = new SyncConf();
        syncConf.setPluginRoot("pluginRoot");
        syncConf.setRemotePluginPath("remotePluginPath");
        syncConf.setSavePointPath("savePointPath");
        syncConf.setSyncJarList(Arrays.asList("syncJarList"));

        String expected =
                "SyncConf{job=null, pluginRoot='pluginRoot', remotePluginPath='remotePluginPath', savePointPath='savePointPath', syncJarList=syncJarList}";
        assertEquals(expected, syncConf.toString());
    }

    @Test
    @DisplayName("Should return the correct string when pluginroot is null")
    public void toStringWhenPluginRootIsNull() {
        SyncConf syncConf = new SyncConf();
        syncConf.setPluginRoot(null);
        syncConf.setRemotePluginPath("remotePluginPath");
        syncConf.setSavePointPath("savePointPath");
        syncConf.setSyncJarList(null);

        String expected =
                "SyncConf{pluginRoot='null', remotePluginPath='remotePluginPath', savePointPath='savePointPath', syncJarList=null}";

        assertEquals(expected, syncConf.asString());
    }

    @Test
    @DisplayName("Should set job")
    public void getOldDirtyConfWhenJobIsNullThenReturnNull() {
        SyncConf syncConf = new SyncConf();
        syncConf.setJob(null);
    }

    @Test
    @DisplayName("Should return the metricpluginconf when the metricpluginconf is not null")
    public void getMetricPluginConfWhenMetricPluginConfIsNotNull() {
        SyncConf syncConf = new SyncConf();
        JobConf jobConf = new JobConf();
        SettingConf settingConf = new SettingConf();
        MetricPluginConf metricPluginConf = new MetricPluginConf();
        settingConf.setMetricPluginConf(metricPluginConf);
        jobConf.setSetting(settingConf);
        syncConf.setJob(jobConf);

        MetricPluginConf result = syncConf.getMetricPluginConf();

        assertNotNull(result);
    }

    @Test
    @DisplayName("Should set the savepointpath when the savepointpath is not null")
    public void setSavePointPathWhenSavePointPathIsNotNull() {
        SyncConf syncConf = new SyncConf();
        syncConf.setSavePointPath("/tmp/savepoint");
        assertNotNull(syncConf.getSavePointPath());
    }

    @Test
    @DisplayName("Should set the pluginroot when the pluginroot is not null")
    public void setPluginRootWhenPluginRootIsNotNull() {
        SyncConf syncConf = new SyncConf();
        syncConf.setPluginRoot("/tmp/plugin");
        assertNotNull(syncConf.getPluginRoot());
    }

    @Test
    @DisplayName("Should return the log when the log is not null")
    public void getLogWhenLogIsNotNull() {
        SyncConf syncConf = new SyncConf();
        syncConf.setJob(new JobConf());
        syncConf.getJob().setSetting(new SettingConf());
        syncConf.getJob().getSetting().setLog(new LogConf());

        LogConf log = syncConf.getLog();

        assertNotNull(log);
    }

    @Test
    @DisplayName("Should return the default log when the log is null")
    public void getLogWhenLogIsNullThenReturnDefaultLog() {
        SyncConf syncConf = new SyncConf();
        syncConf.setJob(new JobConf());
        syncConf.getJob().setSetting(new SettingConf());
        assertNotNull(syncConf.getLog());
    }

    @Test
    @DisplayName("Should return the speed when the speed is not null")
    public void getSpeedWhenSpeedIsNotNull() {
        SyncConf syncConf = new SyncConf();
        JobConf jobConf = new JobConf();
        SettingConf settingConf = new SettingConf();
        SpeedConf speedConf = new SpeedConf();
        speedConf.setChannel(1);
        settingConf.setSpeed(speedConf);
        jobConf.setSetting(settingConf);
        syncConf.setJob(jobConf);

        SpeedConf speed = syncConf.getSpeed();

        assertNotNull(speed);
    }

    @Test
    @DisplayName("Should throw an exception when the reader is null")
    void getReaderWhenReaderIsNullThenThrowException() {
        SyncConf syncConf = new SyncConf();
        syncConf.setJob(new JobConf());

        assertThrows(NullPointerException.class, () -> syncConf.getReader());
    }

    @Test
    @DisplayName("Should throw an exception when content is empty")
    void checkJobWhenContentIsEmptyThenThrowException() {
        String jobJson = "{\"job\":{\"content\":[]}}";

        Throwable exception =
                assertThrows(IllegalArgumentException.class, () -> SyncConf.parseJob(jobJson));

        assertEquals(
                "[content] in the task script is empty, please check the task script configuration.",
                exception.getMessage());
    }
}
