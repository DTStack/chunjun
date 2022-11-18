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

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.List;
import java.util.Locale;

@EqualsAndHashCode(callSuper = true)
@Data
public class InfluxdbSinkConfig extends InfluxdbConfig {

    private static final long serialVersionUID = 1267698737855055756L;

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

    private String timestamp;

    private int writeTimeout = 5;

    /** precision of Unix time */
    private String precision = "ns";

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
}
