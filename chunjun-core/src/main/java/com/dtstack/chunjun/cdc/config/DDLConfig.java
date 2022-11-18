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

package com.dtstack.chunjun.cdc.config;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.Properties;
import java.util.StringJoiner;

public class DDLConfig implements Serializable {

    @JsonProperty("type")
    private String type;

    @JsonProperty("fetchInterval")
    private int fetchInterval = 5000;

    @JsonProperty("properties")
    private Properties properties;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public int getFetchInterval() {
        return fetchInterval;
    }

    public void setFetchInterval(int fetchInterval) {
        this.fetchInterval = fetchInterval;
    }

    public Properties getProperties() {
        return properties;
    }

    public void setProperties(Properties properties) {
        this.properties = properties;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", DDLConfig.class.getSimpleName() + "[", "]")
                .add("type='" + type + "'")
                .add("fetchInterval=" + fetchInterval)
                .add("properties=" + properties)
                .toString();
    }
}
