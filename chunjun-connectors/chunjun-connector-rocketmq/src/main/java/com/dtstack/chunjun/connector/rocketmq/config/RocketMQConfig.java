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

import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.Locale;

public class RocketMQConfig extends CommonConfig {

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

    // producer

    public String getAccessKey() {
        return accessKey;
    }

    public void setAccessKey(String accessKey) {
        this.accessKey = accessKey;
    }

    public String getSecretKey() {
        return secretKey;
    }

    public void setSecretKey(String secretKey) {
        this.secretKey = secretKey;
    }

    public long getPartitionDiscoveryIntervalMs() {
        return partitionDiscoveryIntervalMs;
    }

    public void setPartitionDiscoveryIntervalMs(long partitionDiscoveryIntervalMs) {
        this.partitionDiscoveryIntervalMs = partitionDiscoveryIntervalMs;
    }

    public long getStartMessageTimeStamp() {
        return startMessageTimeStamp;
    }

    public void setStartMessageTimeStamp(long startMessageTimeStamp) {
        this.startMessageTimeStamp = startMessageTimeStamp;
    }

    public long getStartTimeMs() {
        return startTimeMs;
    }

    public void setStartTimeMs(long startTimeMs) {
        this.startTimeMs = startTimeMs;
    }

    public long getEndTimeMs() {
        return endTimeMs;
    }

    public void setEndTimeMs(long endTimeMs) {
        this.endTimeMs = endTimeMs;
    }

    public int getFetchSize() {
        return fetchSize;
    }

    public void setFetchSize(int fetchSize) {
        this.fetchSize = fetchSize;
    }

    public int getHeartbeatBrokerInterval() {
        return heartbeatBrokerInterval;
    }

    public void setHeartbeatBrokerInterval(int heartbeatBrokerInterval) {
        this.heartbeatBrokerInterval = heartbeatBrokerInterval;
    }

    public String getAccessChannel() {
        return accessChannel;
    }

    public void setAccessChannel(String accessChannel) {
        this.accessChannel = accessChannel;
    }

    public String getUnitName() {
        return unitName;
    }

    public void setUnitName(String unitName) {
        this.unitName = unitName;
    }

    public int getPersistConsumerOffsetInterval() {
        return persistConsumerOffsetInterval;
    }

    public void setPersistConsumerOffsetInterval(int persistConsumerOffsetInterval) {
        this.persistConsumerOffsetInterval = persistConsumerOffsetInterval;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public String getNameserverAddress() {
        return nameserverAddress;
    }

    public void setNameserverAddress(String nameserverAddress) {
        this.nameserverAddress = nameserverAddress;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public StartMode getMode() {
        return mode;
    }

    public void setMode(StartMode mode) {
        this.mode = mode;
    }

    public long getStartMessageOffset() {
        return startMessageOffset;
    }

    public void setStartMessageOffset(long startMessageOffset) {
        this.startMessageOffset = startMessageOffset;
    }

    public String getTimeZone() {
        return timeZone;
    }

    public void setTimeZone(String timeZone) {
        this.timeZone = timeZone;
    }

    public String getEncoding() {
        return encoding;
    }

    public void setEncoding(String encoding) {
        this.encoding = encoding;
    }

    public String getFieldDelimiter() {
        return fieldDelimiter;
    }

    public void setFieldDelimiter(String fieldDelimiter) {
        this.fieldDelimiter = fieldDelimiter;
    }

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

    public static class Builder implements Serializable {
        private static final long serialVersionUID = 1L;
        private final RocketMQConfig conf;

        public Builder() {
            this.conf = new RocketMQConfig();
        }

        public Builder setTopic(String topic) {
            this.conf.setTopic(topic);
            return this;
        }

        public Builder setNameserverAddress(String nameserverAddress) {
            this.conf.setNameserverAddress(nameserverAddress);
            return this;
        }

        public Builder setHeartbeatBrokerInterval(int heartbeatBrokerInterval) {
            this.conf.setHeartbeatBrokerInterval(heartbeatBrokerInterval);
            return this;
        }

        public Builder setAccessChannel(String accessChannel) {
            this.conf.setAccessChannel(accessChannel);
            return this;
        }

        public Builder setUnitName(String unitName) {
            this.conf.setUnitName(unitName);
            return this;
        }

        public Builder setTag(String tag) {
            this.conf.setTag(tag);
            return this;
        }

        public Builder setConsumerGroup(String consumerGroup) {
            this.conf.setConsumerGroup(consumerGroup);
            return this;
        }

        public Builder setMode(StartMode mode) {
            this.conf.setMode(mode);
            return this;
        }

        public Builder setStartMessageOffset(long startMessageOffset) {
            this.conf.setStartMessageOffset(startMessageOffset);
            return this;
        }

        public Builder setStartTimeMs(long startTimeMs) {
            this.conf.setStartTimeMs(startTimeMs);
            return this;
        }

        public Builder setEndTimeMs(long endTimeMs) {
            this.conf.setEndTimeMs(endTimeMs);
            return this;
        }

        public Builder setTimeZone(String timeZone) {
            this.conf.setTimeZone(timeZone);
            return this;
        }

        public Builder setEncoding(String encoding) {
            this.conf.setEncoding(encoding);
            return this;
        }

        public Builder setFieldDelimiter(String fieldDelimiter) {
            this.conf.setFieldDelimiter(fieldDelimiter);
            return this;
        }

        public Builder setPersistConsumerOffsetInterval(int persistConsumerOffsetInterval) {
            this.conf.setPersistConsumerOffsetInterval(persistConsumerOffsetInterval);
            return this;
        }

        public Builder setStartMessageTimeStamp(long startMessageTimeStamp) {
            this.conf.setStartMessageTimeStamp(startMessageTimeStamp);
            return this;
        }

        public Builder setFetchSize(int fetchSize) {
            this.conf.setFetchSize(fetchSize);
            return this;
        }

        public Builder setPartitionDiscoveryIntervalMs(long partitionDiscoveryIntervalMs) {
            this.conf.setPartitionDiscoveryIntervalMs(partitionDiscoveryIntervalMs);
            return this;
        }

        public Builder setAccessKey(String accessKey) {
            this.conf.setAccessKey(accessKey);
            return this;
        }

        public Builder setSecretKey(String secretKey) {
            this.conf.setSecretKey(secretKey);
            return this;
        }

        public RocketMQConfig build() {
            return this.conf;
        }
    }
}
