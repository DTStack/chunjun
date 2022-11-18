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
package com.dtstack.chunjun.config;

import java.io.Serializable;
import java.util.StringJoiner;

public class SettingConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    /** 速率及通道配置 */
    private SpeedConfig speed = new SpeedConfig();
    /** 任务指标插件信息 */
    private MetricPluginConfig metricPluginConfig = new MetricPluginConfig();
    /** 断点续传配置 */
    private RestoreConfig restore = new RestoreConfig();
    /** 失败重试配置 */
    private RestartConfig restart = new RestartConfig();
    /** ChunJun日志记录配置 */
    private LogConfig log = new LogConfig();

    public void setMetricPluginConfig(MetricPluginConfig metricPluginConfig) {
        this.metricPluginConfig = metricPluginConfig;
    }

    public MetricPluginConfig getMetricPluginConfig() {
        return metricPluginConfig;
    }

    public SpeedConfig getSpeed() {
        return speed;
    }

    public void setSpeed(SpeedConfig speed) {
        this.speed = speed;
    }

    public RestoreConfig getRestore() {
        return restore;
    }

    public void setRestore(RestoreConfig restore) {
        this.restore = restore;
    }

    public RestartConfig getRestart() {
        return restart;
    }

    public void setRestart(RestartConfig restart) {
        this.restart = restart;
    }

    public LogConfig getLog() {
        return log;
    }

    public void setLog(LogConfig log) {
        this.log = log;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", SettingConfig.class.getSimpleName() + "[", "]")
                .add("speed=" + speed)
                .add("metricPluginConf=" + metricPluginConfig)
                .add("restore=" + restore)
                .add("restart=" + restart)
                .add("log=" + log)
                .toString();
    }
}
