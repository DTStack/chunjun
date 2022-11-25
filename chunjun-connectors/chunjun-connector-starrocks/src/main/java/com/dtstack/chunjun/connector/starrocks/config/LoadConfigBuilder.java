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

package com.dtstack.chunjun.connector.starrocks.config;

import java.util.Map;

public class LoadConfigBuilder {
    private final LoadConfig loadConfig;

    public LoadConfigBuilder() {
        this.loadConfig = new LoadConfig();
    }

    public LoadConfig build() {
        return this.loadConfig;
    }

    public LoadConfigBuilder setBatchMaxSize(Long batchMaxSize) {
        loadConfig.setBatchMaxSize(batchMaxSize);
        return this;
    }

    public LoadConfigBuilder setBatchMaxRows(Long batMaxRows) {
        loadConfig.setBatchMaxRows(batMaxRows);
        return this;
    }

    public LoadConfigBuilder setHttpCheckTimeoutMs(int httpCheckTimeout) {
        loadConfig.setHttpCheckTimeoutMs(httpCheckTimeout);
        return this;
    }

    public LoadConfigBuilder setQueueOfferTimeoutMs(int queueOfferTimeoutMs) {
        loadConfig.setQueueOfferTimeoutMs(queueOfferTimeoutMs);
        return this;
    }

    public LoadConfigBuilder setQueuePollTimeoutMs(int queuePollTimeoutMs) {
        loadConfig.setQueuePollTimeoutMs(queuePollTimeoutMs);
        return this;
    }

    public LoadConfigBuilder setHeadProperties(Map<String, String> loadProperties) {
        loadConfig.setHeadProperties(loadProperties);
        return this;
    }
}
