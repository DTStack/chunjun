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

package com.dtstack.flinkx.connector.dorisbatch.options;

import java.util.List;
import java.util.Properties;
import java.util.StringJoiner;

/**
 * @author tiezhu@dtstack
 * @date 2021/9/17 星期五
 */
public class DorisOptionsBuilder {
    private final DorisOptions options;

    public DorisOptionsBuilder() {
        this.options = new DorisOptions();
    }

    public DorisOptionsBuilder setDatabase(String database) {
        this.options.setDatabase(database);
        return this;
    }

    public DorisOptionsBuilder setTable(String table) {
        this.options.setTable(table);
        return this;
    }

    public DorisOptionsBuilder setFeNodes(List<String> feNodes) {
        this.options.setFeNodes(feNodes);
        return this;
    }

    public DorisOptionsBuilder setFieldDelimiter(String fieldDelimiter) {
        this.options.setFieldDelimiter(fieldDelimiter);
        return this;
    }

    public DorisOptionsBuilder setLineDelimiter(String lineDelimiter) {
        this.options.setLineDelimiter(lineDelimiter);
        return this;
    }

    public DorisOptionsBuilder setUsername(String username) {
        this.options.setUsername(username);
        return this;
    }

    public DorisOptionsBuilder setPassword(String password) {
        this.options.setPassword(password);
        return this;
    }

    public DorisOptionsBuilder setWriteMode(String writeMode) {
        this.options.setWriteMode(writeMode);
        return this;
    }

    public DorisOptionsBuilder setMaxRetries(Integer maxRetries) {
        this.options.setMaxRetries(maxRetries);
        return this;
    }

    public DorisOptionsBuilder setLoadOptions(LoadOptions loadOptions) {
        this.options.setLoadOptions(loadOptions);
        return this;
    }

    public DorisOptionsBuilder setLoadProperties(Properties loadProperties) {
        this.options.setLoadProperties(loadProperties);
        return this;
    }

    public DorisOptionsBuilder setBatchSize(int batchSize) {
        this.options.setBatchSize(batchSize);
        return this;
    }

    public DorisOptions build() {
        StringJoiner errorMessage = new StringJoiner("\n");

        if (options.getFeNodes() == null || options.getFeNodes().isEmpty()) {
            errorMessage.add("Doris FeNodes can not be empty!");
        }

        if (options.getUsername() == null || options.getUsername().isEmpty()) {
            errorMessage.add("Doris Username can not be empty!");
        }

        if (errorMessage.length() > 0) {
            throw new IllegalArgumentException("Doris Options error:\n" + errorMessage);
        }

        return options;
    }
}
