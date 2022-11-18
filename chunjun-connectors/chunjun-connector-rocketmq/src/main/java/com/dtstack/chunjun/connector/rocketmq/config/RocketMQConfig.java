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

package com.dtstack.chunjun.connector.rocketmq.config;

import com.dtstack.chunjun.config.CommonConfig;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;

import java.util.Locale;

@Getter
@Setter
@ToString
@Builder
public class RocketMQConfig extends CommonConfig {

    private static final long serialVersionUID = 3283705129428004165L;

    private String topic;
    private String nameserverAddress;
    private String unitName;
    private int heartbeatBrokerInterval = 30000;
    // security
    private String accessKey;
    private String secretKey;
    // for aliyun instance
    private String accessChannel = "LOCAL";

    // consumer
    private String tag;
    private String consumerGroup;
    private StartMode mode = StartMode.EARLIEST;
    private long startMessageOffset;
    private long startMessageTimeStamp = -1L;
    private long startTimeMs = -1L;
    private long endTimeMs = Long.MAX_VALUE;
    private String timeZone;
    private String encoding = "UTF-8";
    private String fieldDelimiter = "\u0001";
    private int fetchSize = 32;
    /** offset persistent interval for consumer* */
    private int persistConsumerOffsetInterval = 5000; // 5s

    private long partitionDiscoveryIntervalMs = 30000; // 30s

    public enum StartMode {
        /** Start from the earliest offset possible. */
        EARLIEST("earliest"),
        /** Start from the latest offset. */
        LATEST("latest"),
        /** Start from user-supplied timestamp for each messageQueue. */
        TIMESTAMP("timestamp"),
        /** Start from user-supplied offset for each messageQueue */
        OFFSET("offset"),

        UNKNOWN("unknown");

        final String name;

        StartMode(String name) {
            this.name = name;
        }

        public static StartMode getFromName(String name) {
            if (StringUtils.isBlank(name)) {
                throw new IllegalArgumentException("mode name is blank.");
            }
            switch (name.toLowerCase(Locale.ENGLISH)) {
                case "earliest":
                    return EARLIEST;
                case "latest":
                    return LATEST;
                case "timestamp":
                    return TIMESTAMP;
                case "offset":
                    return OFFSET;
                default:
                    return UNKNOWN;
            }
        }
    }
}
