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

/**
 * @author tiezhu@dtstack
 * @date 2021/9/18 星期六
 */
public class LoadOptionsBuilder {
    private final LoadOptions options;

    public LoadOptionsBuilder() {
        this.options = new LoadOptions();
    }

    public LoadOptionsBuilder setReadFields(String readFields) {
        this.options.setReadFields(readFields);
        return this;
    }

    public LoadOptionsBuilder setFilterQuery(String filterQuery) {
        this.options.setFilterQuery(filterQuery);
        return this;
    }

    public LoadOptionsBuilder setRequestTabletSize(Integer requestTabletSize) {
        this.options.setRequestBatchSize(requestTabletSize);
        return this;
    }

    public LoadOptionsBuilder setRequestConnectTimeoutMs(Integer requestConnectTimeoutMs) {
        this.options.setRequestConnectTimeoutMs(requestConnectTimeoutMs);
        return this;
    }

    public LoadOptionsBuilder setRequestReadTimeoutMs(Integer requestReadTimeoutMs) {
        this.options.setRequestReadTimeoutMs(requestReadTimeoutMs);
        return this;
    }

    public LoadOptionsBuilder setRequestQueryTimeoutMs(Integer requestQueryTimeoutMs) {
        this.options.setRequestQueryTimeoutS(requestQueryTimeoutMs);
        return this;
    }

    public LoadOptionsBuilder setRequestRetries(Integer requestRetries) {
        this.options.setRequestRetries(requestRetries);
        return this;
    }

    public LoadOptionsBuilder setRequestBatchSize(Integer requestBatchSize) {
        this.options.setRequestBatchSize(requestBatchSize);
        return this;
    }

    public LoadOptionsBuilder setExecMemLimit(Long execMemLimit) {
        this.options.setExecMemLimit(execMemLimit);
        return this;
    }

    public LoadOptionsBuilder setDeserializeQueueSize(Integer deserializeQueueSize) {
        this.options.setDeserializeQueueSize(deserializeQueueSize);
        return this;
    }

    public LoadOptionsBuilder setDeserializeArrowAsync(Boolean deserializeArrowAsync) {
        this.options.setDeserializeArrowAsync(deserializeArrowAsync);
        return this;
    }

    public LoadOptions build() {
        return this.options;
    }
}
