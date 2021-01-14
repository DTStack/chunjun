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
package com.dtstack.flinkx.kafkabase.entity;

import java.io.Serializable;
import java.util.Objects;

/**
 * Date: 2020/12/26
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public class kafkaState implements Serializable {
    private static final long serialVersionUID = 1L;

    private String topic;
    private Integer partition;
    private Long offset;
    private Long timestamp;

    public kafkaState(String topic, Integer partition, Long offset, Long timestamp) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.timestamp = timestamp;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public Integer getPartition() {
        return partition;
    }

    public void setPartition(Integer partition) {
        this.partition = partition;
    }

    public Long getOffset() {
        return offset;
    }

    public void setOffset(Long offset) {
        this.offset = offset;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        kafkaState that = (kafkaState) o;
        return partition == that.partition &&
                offset == that.offset &&
                timestamp == that.timestamp &&
                topic.equals(that.topic);
    }

    @Override
    public kafkaState clone() {
        return new kafkaState(this.topic, this.partition, this.offset, this.timestamp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topic, partition, offset, timestamp);
    }

    @Override
    public String toString() {
        return "kafkaState{" +
                "topic='" + topic + '\'' +
                ", partition=" + partition +
                ", offset=" + offset +
                ", timestamp=" + timestamp +
                '}';
    }
}
