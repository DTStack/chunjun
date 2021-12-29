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

package com.dtstack.flinkx.connector.doris.options;

import java.util.List;
import java.util.Properties;
import java.util.StringJoiner;

/**
 * @author tiezhu@dtstack
 * @date 2021/9/17 星期五
 */
public class DorisConfBuilder {
    private final DorisConf dorisConf;

    public DorisConfBuilder() {
        this.dorisConf = new DorisConf();
    }

    public DorisConfBuilder setDatabase(String database) {
        this.dorisConf.setDatabase(database);
        return this;
    }

    public DorisConfBuilder setTable(String table) {
        this.dorisConf.setTable(table);
        return this;
    }

    public DorisConfBuilder setFeNodes(List<String> feNodes) {
        this.dorisConf.setFeNodes(feNodes);
        return this;
    }

    public DorisConfBuilder setFieldDelimiter(String fieldDelimiter) {
        this.dorisConf.setFieldDelimiter(fieldDelimiter);
        return this;
    }

    public DorisConfBuilder setLineDelimiter(String lineDelimiter) {
        this.dorisConf.setLineDelimiter(lineDelimiter);
        return this;
    }

    public DorisConfBuilder setUsername(String username) {
        this.dorisConf.setUsername(username);
        return this;
    }

    public DorisConfBuilder setPassword(String password) {
        this.dorisConf.setPassword(password);
        return this;
    }

    public DorisConfBuilder setWriteMode(String writeMode) {
        this.dorisConf.setWriteMode(writeMode);
        return this;
    }

    public DorisConfBuilder setMaxRetries(Integer maxRetries) {
        this.dorisConf.setMaxRetries(maxRetries);
        return this;
    }

    public DorisConfBuilder setLoadOptions(LoadConf loadConf) {
        this.dorisConf.setLoadConf(loadConf);
        return this;
    }

    public DorisConfBuilder setLoadProperties(Properties loadProperties) {
        this.dorisConf.setLoadProperties(loadProperties);
        return this;
    }

    public DorisConfBuilder setBatchSize(int batchSize) {
        this.dorisConf.setBatchSize(batchSize);
        return this;
    }

    public DorisConf build() {
        StringJoiner errorMessage = new StringJoiner("\n");

        if (dorisConf.getFeNodes() == null || dorisConf.getFeNodes().isEmpty()) {
            errorMessage.add("Doris FeNodes can not be empty!");
        }

        if (dorisConf.getUsername() == null || dorisConf.getUsername().isEmpty()) {
            errorMessage.add("Doris Username can not be empty!");
        }

        if (errorMessage.length() > 0) {
            throw new IllegalArgumentException("Doris Options error:\n" + errorMessage);
        }

        return dorisConf;
    }
}
