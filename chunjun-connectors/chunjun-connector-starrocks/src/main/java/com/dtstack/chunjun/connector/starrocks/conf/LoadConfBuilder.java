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

package com.dtstack.chunjun.connector.starrocks.conf;

import java.util.Map;

/** @author liuliu 2022/7/12 */
public class LoadConfBuilder {
    private final LoadConf loadConf;

    public LoadConfBuilder() {
        this.loadConf = new LoadConf();
    }

    public LoadConf build() {
        return this.loadConf;
    }

    public LoadConfBuilder setBatchMaxSize(Long batchMaxSize) {
        loadConf.setBatchMaxSize(batchMaxSize);
        return this;
    }

    public LoadConfBuilder setBatchMaxRows(Long batMaxRows) {
        loadConf.setBatchMaxRows(batMaxRows);
        return this;
    }

    public LoadConfBuilder setHttpCheckTimeoutMs(int httpCheckTimeout) {
        loadConf.setHttpCheckTimeoutMs(httpCheckTimeout);
        return this;
    }

    public LoadConfBuilder setQueueOfferTimeoutMs(int queueOfferTimeoutMs) {
        loadConf.setQueueOfferTimeoutMs(queueOfferTimeoutMs);
        return this;
    }

    public LoadConfBuilder setQueuePollTimeoutMs(int queuePollTimeoutMs) {
        loadConf.setQueuePollTimeoutMs(queuePollTimeoutMs);
        return this;
    }

    public LoadConfBuilder setHeadProperties(Map<String, String> loadProperties) {
        loadConf.setHeadProperties(loadProperties);
        return this;
    }
}
