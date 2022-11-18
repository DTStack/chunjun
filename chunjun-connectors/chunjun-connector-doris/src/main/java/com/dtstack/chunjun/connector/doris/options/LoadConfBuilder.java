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

package com.dtstack.chunjun.connector.doris.options;

public class LoadConfBuilder {
    private final LoadConfig loadConfig;

    public LoadConfBuilder() {
        this.loadConfig = new LoadConfig();
    }

    public LoadConfBuilder setReadFields(String readFields) {
        this.loadConfig.setReadFields(readFields);
        return this;
    }

    public LoadConfBuilder setFilterQuery(String filterQuery) {
        this.loadConfig.setFilterQuery(filterQuery);
        return this;
    }

    public LoadConfBuilder setRequestTabletSize(Integer requestTabletSize) {
        this.loadConfig.setRequestBatchSize(requestTabletSize);
        return this;
    }

    public LoadConfBuilder setRequestConnectTimeoutMs(Integer requestConnectTimeoutMs) {
        this.loadConfig.setRequestConnectTimeoutMs(requestConnectTimeoutMs);
        return this;
    }

    public LoadConfBuilder setRequestReadTimeoutMs(Integer requestReadTimeoutMs) {
        this.loadConfig.setRequestReadTimeoutMs(requestReadTimeoutMs);
        return this;
    }

    public LoadConfBuilder setRequestQueryTimeoutMs(Integer requestQueryTimeoutMs) {
        this.loadConfig.setRequestQueryTimeoutS(requestQueryTimeoutMs);
        return this;
    }

    public LoadConfBuilder setRequestRetries(Integer requestRetries) {
        this.loadConfig.setRequestRetries(requestRetries);
        return this;
    }

    public LoadConfBuilder setRequestBatchSize(Integer requestBatchSize) {
        this.loadConfig.setRequestBatchSize(requestBatchSize);
        return this;
    }

    public LoadConfBuilder setExecMemLimit(Long execMemLimit) {
        this.loadConfig.setExecMemLimit(execMemLimit);
        return this;
    }

    public LoadConfBuilder setDeserializeQueueSize(Integer deserializeQueueSize) {
        this.loadConfig.setDeserializeQueueSize(deserializeQueueSize);
        return this;
    }

    public LoadConfBuilder setDeserializeArrowAsync(Boolean deserializeArrowAsync) {
        this.loadConfig.setDeserializeArrowAsync(deserializeArrowAsync);
        return this;
    }

    public LoadConfig build() {
        return this.loadConfig;
    }
}
