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
package com.dtstack.chunjun.connector.http.common;

import org.apache.http.HttpResponse;
import org.apache.http.client.ServiceUnavailableRetryStrategy;
import org.apache.http.protocol.HttpContext;

/** 自定义httpClient重试策略,默认重试次数为5,重试时间间隔为2s */
public class ServiceUnavailableRetryStrategyImpl implements ServiceUnavailableRetryStrategy {
    private final int executionCount;
    private final long retryInterval;

    public ServiceUnavailableRetryStrategyImpl(Builder builder) {
        this.executionCount = builder.executionCount;
        this.retryInterval = builder.retryInterval;
    }

    @Override
    public boolean retryRequest(
            HttpResponse httpResponse, int executionCount, HttpContext httpContext) {
        int successCode = 200;
        return httpResponse.getStatusLine().getStatusCode() != successCode
                && executionCount < this.executionCount;
    }

    @Override
    public long getRetryInterval() {
        return this.retryInterval;
    }

    public static final class Builder {
        private int executionCount;
        private long retryInterval;

        public Builder() {
            executionCount = 5;
            retryInterval = 2000;
        }

        public Builder executionCount(int executionCount) {
            this.executionCount = executionCount;
            return this;
        }

        public Builder retryInterval(long retryInterval) {
            this.retryInterval = retryInterval;
            return this;
        }

        public ServiceUnavailableRetryStrategyImpl build() {
            return new ServiceUnavailableRetryStrategyImpl(this);
        }
    }
}
