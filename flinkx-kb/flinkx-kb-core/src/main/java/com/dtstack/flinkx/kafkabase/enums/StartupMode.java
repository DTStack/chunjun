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

package com.dtstack.flinkx.kafkabase.enums;

import org.apache.commons.lang3.StringUtils;

import java.util.Locale;

/**
 * Date: 2019/11/21
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public enum StartupMode {

    /**
     * Start from committed offsets in ZK / Kafka brokers of a specific consumer group (default).
     */
    GROUP_OFFSETS("group-offsets"),
    /**
     * Start from the earliest offset possible.
     */
    EARLIEST("earliest-offset"),
    /**
     * Start from the latest offset.
     */
    LATEST("latest-offset"),
    /**
     * Start from user-supplied timestamp for each partition.
     */
    TIMESTAMP("timestamp"),
    /**
     * Start from user-supplied specific offsets for each partition
     */
    SPECIFIC_OFFSETS("specific-offsets"),

    UNKNOWN("unknown");

    public String name;

    StartupMode(String name) {
        this.name = name;
    }

    /**
     * 根据名称获取启动模式
     * @param name
     * @return
     */
    public static StartupMode getFromName(String name){
        if(StringUtils.isBlank(name)){
            throw new IllegalArgumentException("StartupMode name is blank.");
        }
        switch (name.toLowerCase(Locale.ENGLISH)){
            case "earliest-offset": return EARLIEST;
            case "latest-offset": return LATEST;
            case "timestamp": return TIMESTAMP;
            case "specific-offsets": return SPECIFIC_OFFSETS;
            case "group-offsets": return GROUP_OFFSETS;
            default: return UNKNOWN;
        }
    }
}
