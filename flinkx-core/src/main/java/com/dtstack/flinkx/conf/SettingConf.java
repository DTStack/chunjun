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
package com.dtstack.flinkx.conf;

import java.io.Serializable;

/**
 * Date: 2021/01/18 Company: www.dtstack.com
 *
 * @author tudou
 */
public class SettingConf implements Serializable {
    private static final long serialVersionUID = 1L;

    /** 速率及通道配置 */
    private SpeedConf speed = new SpeedConf();
    /** 任务运行时数据读取写入的出错控制 */
    private ErrorLimitConf errorLimit = new ErrorLimitConf();
    /** 任务指标插件信息 */
    private MetricPluginConf metricPluginConf = new MetricPluginConf();
    /** 断点续传配置 */
    private RestoreConf restore = new RestoreConf();
    /** 失败重试配置 */
    private RestartConf restart = new RestartConf();
    /** FlinkX日志记录配置 */
    private LogConf log = new LogConf();

    public void setMetricPluginConf(MetricPluginConf metricPluginConf) {
        this.metricPluginConf = metricPluginConf;
    }

    public MetricPluginConf getMetricPluginConf() {
        return metricPluginConf;
    }

    public SpeedConf getSpeed() {
        return speed;
    }

    public void setSpeed(SpeedConf speed) {
        this.speed = speed;
    }

    public ErrorLimitConf getErrorLimit() {
        return errorLimit;
    }

    public void setErrorLimit(ErrorLimitConf errorLimit) {
        this.errorLimit = errorLimit;
    }

    public RestoreConf getRestore() {
        return restore;
    }

    public void setRestore(RestoreConf restore) {
        this.restore = restore;
    }

    public RestartConf getRestart() {
        return restart;
    }

    public void setRestart(RestartConf restart) {
        this.restart = restart;
    }

    public LogConf getLog() {
        return log;
    }

    public void setLog(LogConf log) {
        this.log = log;
    }

    @Override
    public String toString() {
        return "SettingConf{"
                + "speed="
                + speed
                + ", errorLimit="
                + errorLimit
                + ", metricPluginConf="
                + metricPluginConf
                + ", restore="
                + restore
                + ", restart="
                + restart
                + ", log="
                + log
                + '}';
    }
}
