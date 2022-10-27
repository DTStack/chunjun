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
    private final LoadConf loadConf;

    public LoadConfBuilder() {
        this.loadConf = new LoadConf();
    }

    public LoadConfBuilder setReadFields(String readFields) {
        this.loadConf.setReadFields(readFields);
        return this;
    }

    public LoadConfBuilder setFilterQuery(String filterQuery) {
        this.loadConf.setFilterQuery(filterQuery);
        return this;
    }

    public LoadConfBuilder setRequestTabletSize(Integer requestTabletSize) {
        this.loadConf.setRequestBatchSize(requestTabletSize);
        return this;
    }

    public LoadConfBuilder setRequestConnectTimeoutMs(Integer requestConnectTimeoutMs) {
        this.loadConf.setRequestConnectTimeoutMs(requestConnectTimeoutMs);
        return this;
    }

    public LoadConfBuilder setRequestReadTimeoutMs(Integer requestReadTimeoutMs) {
        this.loadConf.setRequestReadTimeoutMs(requestReadTimeoutMs);
        return this;
    }

    public LoadConfBuilder setRequestQueryTimeoutMs(Integer requestQueryTimeoutMs) {
        this.loadConf.setRequestQueryTimeoutS(requestQueryTimeoutMs);
        return this;
    }

    public LoadConfBuilder setRequestRetries(Integer requestRetries) {
        this.loadConf.setRequestRetries(requestRetries);
        return this;
    }

    public LoadConfBuilder setRequestBatchSize(Integer requestBatchSize) {
        this.loadConf.setRequestBatchSize(requestBatchSize);
        return this;
    }

    public LoadConfBuilder setExecMemLimit(Long execMemLimit) {
        this.loadConf.setExecMemLimit(execMemLimit);
        return this;
    }

    public LoadConfBuilder setDeserializeQueueSize(Integer deserializeQueueSize) {
        this.loadConf.setDeserializeQueueSize(deserializeQueueSize);
        return this;
    }

    public LoadConfBuilder setDeserializeArrowAsync(Boolean deserializeArrowAsync) {
        this.loadConf.setDeserializeArrowAsync(deserializeArrowAsync);
        return this;
    }

    public LoadConf build() {
        return this.loadConf;
    }
}
