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

import java.util.HashMap;
import java.util.Map;

/**
 * Date: 2019/12/17
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public class LogConfig extends AbstractConfig {
    public static final String KEY_ISLOGGER = "isLogger";
    public static final String KEY_LEVEL = "level";
    public static final String KEY_PATH = "path";
    public static final String KEY_PATTERN = "pattern";

    public static final boolean DEFAULT_ISLOGGER = false;
    public static final String DEFAULT_LEVEL = "info";
    public static final String DEFAULT_PATH = "/tmp/dtstack/flinkx/";
    public static final String DEFAULT_LOG4J_PATTERN = "%d{yyyy-MM-dd HH:mm:ss,SSS} %-5p %-60c %x - %m%n";
    public static final String DEFAULT_LOGBACK_PATTERN = "%d{yyyy-MM-dd HH:mm:ss.SSS} [%thread] %-5level %logger{60} %X{sourceThread} - %msg%n";


    public LogConfig(Map<String, Object> map) {
        super(map);
    }

    public static LogConfig defaultConfig(){
        Map<String, Object> map = new HashMap<>(8);
        map.put(KEY_ISLOGGER,DEFAULT_ISLOGGER);
        map.put(KEY_LEVEL,DEFAULT_LEVEL);
        map.put(KEY_PATH,DEFAULT_PATH);
        return new LogConfig(map);
    }

    public boolean isLogger(){
        return getBooleanVal(KEY_ISLOGGER, DEFAULT_ISLOGGER);
    }

    public String getLevel() {
        return getStringVal(KEY_LEVEL, DEFAULT_LEVEL);
    }

    public String getPath() {
        return getStringVal(KEY_PATH, DEFAULT_PATH);
    }

    public String getPattern() {
        return getStringVal(KEY_PATTERN);
    }
}
