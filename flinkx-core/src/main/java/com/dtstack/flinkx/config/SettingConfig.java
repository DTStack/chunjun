/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.flinkx.config;

import java.util.Map;

/**
 * The configuration of setting optoins
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class SettingConfig extends AbstractConfig {

    public static final String KEY_SPEED_CONFIG = "speed";

    public static final String KEY_ERROR_LIMIT_CONFIG = "errorLimit";

    public static final String KEY_DIRTY_CONFIG = "dirty";

    public static final String KEY_RESTORE = "restore";

    public static final String KEY_TEST = "test";

    public static final String KEY_RESTART = "restart";

    public static final String KEY_LOG = "log";

    private SpeedConfig speed = SpeedConfig.defaultConfig();

    private ErrorLimitConfig errorLimit = ErrorLimitConfig.defaultConfig();

    private RestoreConfig restoreConfig = RestoreConfig.defaultConfig();

    private TestConfig testConfig = TestConfig.defaultConfig();

    private RestartConfig restartConfig = RestartConfig.defaultConfig();

    private LogConfig logConfig = LogConfig.defaultConfig();

    private DirtyConfig dirty;

    @SuppressWarnings("unchecked")
    public SettingConfig(Map<String, Object> map) {
        super(map);
        if(map.containsKey(KEY_SPEED_CONFIG)) {
            speed = new SpeedConfig((Map<String, Object>) map.get(KEY_SPEED_CONFIG));
        }
        if(map.containsKey(KEY_ERROR_LIMIT_CONFIG)) {
            errorLimit = new ErrorLimitConfig((Map<String, Object>) map.get(KEY_ERROR_LIMIT_CONFIG));
        }
        if(map.containsKey(KEY_DIRTY_CONFIG)) {
            dirty = new DirtyConfig((Map<String, Object>) map.get(KEY_DIRTY_CONFIG));
        }

        if (map.containsKey(KEY_RESTORE)){
            restoreConfig = new RestoreConfig((Map<String, Object>) map.get(KEY_RESTORE));
        }

        if (map.containsKey(KEY_LOG)){
            logConfig = new LogConfig((Map<String, Object>) map.get(KEY_LOG));
        }

        if (map.containsKey(KEY_TEST)) {
            testConfig = new TestConfig((Map<String, Object>) map.get(KEY_TEST));
        }

        if (map.containsKey(KEY_RESTART)) {
            restartConfig = new RestartConfig((Map<String, Object>) map.get(KEY_RESTART));
        }
    }

    public SpeedConfig getSpeed() {
        return speed;
    }

    public void setSpeed(SpeedConfig speed) {
        this.speed = speed;
    }

    public ErrorLimitConfig getErrorLimit() {
        return errorLimit;
    }

    public void setErrorLimit(ErrorLimitConfig errorLimit) {
        this.errorLimit = errorLimit;
    }

    public RestoreConfig getRestoreConfig() {
        return restoreConfig;
    }

    public void setRestoreConfig(RestoreConfig restoreConfig) {
        this.restoreConfig = restoreConfig;
    }

    public LogConfig getLogConfig() {
        return logConfig;
    }

    public void setLogConfig(LogConfig logConfig) {
        this.logConfig = logConfig;
    }

    public DirtyConfig getDirty() {
        return dirty;
    }

    public void setDirty(DirtyConfig dirty) {
        this.dirty = dirty;
    }

    public TestConfig getTestConfig() {
        return testConfig;
    }

    public void setTestConfig(TestConfig testConfig) {
        this.testConfig = testConfig;
    }

    public RestartConfig getRestartConfig() {
        return restartConfig;
    }

    public void setRestartConfig(RestartConfig restartConfig) {
        this.restartConfig = restartConfig;
    }
}

