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

import com.dtstack.flinkx.constants.ConfigConstant;

import java.io.Serializable;

/**
 * Date: 2021/01/18 Company: www.dtstack.com
 *
 * @author tudou
 */
public class LogConf implements Serializable {
    private static final long serialVersionUID = 1L;

    /** 是否开启FlinkX日志记录，默认不开启 */
    private boolean isLogger = false;
    /** 日志记录的日志级别 */
    private String level = "info";
    /** 本地日志保存路径，路径不存在会自动创建 */
    private String path = "/tmp/dtstack/flinkx/";
    /** 日志格式，默认为log4j格式 */
    private String pattern = ConfigConstant.DEFAULT_LOG4J_PATTERN;

    public boolean isLogger() {
        return isLogger;
    }

    public void setLogger(boolean logger) {
        isLogger = logger;
    }

    public String getLevel() {
        return level;
    }

    public void setLevel(String level) {
        this.level = level;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getPattern() {
        return pattern;
    }

    public void setPattern(String pattern) {
        this.pattern = pattern;
    }

    @Override
    public String toString() {
        return "LogConf{"
                + "isLogger="
                + isLogger
                + ", level='"
                + level
                + '\''
                + ", path='"
                + path
                + '\''
                + ", pattern='"
                + pattern
                + '\''
                + '}';
    }
}
