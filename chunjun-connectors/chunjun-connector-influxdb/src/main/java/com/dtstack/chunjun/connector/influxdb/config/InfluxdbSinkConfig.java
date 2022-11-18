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

package com.dtstack.chunjun.connector.influxdb.config;

import com.dtstack.chunjun.sink.WriteMode;

import java.util.List;
import java.util.Locale;

public class InfluxdbSinkConfig extends InfluxdbConfig {

    /** retention policy for influxdb writer */
    private String rp;

    /** write mode for influxdb writer */
    private WriteMode writeMode = WriteMode.INSERT;

    /** tags of the measurement */
    private List<String> tags;

    private int bufferLimit = 10000;

    /** flush duration (ms) */
    private int flushDuration = 1000;

    private boolean enableBatch = true;

    public boolean isEnableBatch() {
        return enableBatch;
    }

    public void setEnableBatch(boolean enableBatch) {
        this.enableBatch = enableBatch;
    }

    public int getFlushDuration() {
        return flushDuration;
    }

    public void setFlushDuration(int flushDuration) {
        this.flushDuration = flushDuration;
    }

    /** the name of timestamp field */
    private String timestamp;

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    private int writeTimeout = 5;

    public int getWriteTimeout() {
        return writeTimeout;
    }

    public void setWriteTimeout(int writeTimeout) {
        this.writeTimeout = writeTimeout;
    }

    /** precision of Unix time */
    private String precision = "ns";

    public String getRp() {
        return rp;
    }

    public void setRp(String rp) {
        this.rp = rp;
    }

    public WriteMode getWriteMode() {
        return writeMode;
    }

    public void setWriteMode(String writeMode) {
        switch (writeMode.toLowerCase(Locale.ENGLISH)) {
            case "insert":
                this.writeMode = WriteMode.INSERT;
                break;
            case "update":
                this.writeMode = WriteMode.UPDATE;
                break;
            case "upsert":
                this.writeMode = WriteMode.UPSERT;
                break;
            default:
                this.writeMode = WriteMode.APPEND;
        }
    }

    public List<String> getTags() {
        return tags;
    }

    public void setTags(List<String> tags) {
        this.tags = tags;
    }

    public int getBufferLimit() {
        return bufferLimit;
    }

    public void setBufferLimit(int bufferLimit) {
        this.bufferLimit = bufferLimit;
    }

    public String getPrecision() {
        return precision;
    }

    public void setPrecision(String precision) {
        this.precision = precision;
    }
}
