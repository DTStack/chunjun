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

import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpRequest;
import org.apache.http.NoHttpResponseException;
import org.apache.http.client.HttpRequestRetryHandler;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.protocol.HttpContext;

import javax.net.ssl.SSLException;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.UnknownHostException;

@Slf4j
public class HttpRequestRetryHandlerImpl implements HttpRequestRetryHandler {

    private final int executionMaxCount;

    public HttpRequestRetryHandlerImpl(Builder builder) {
        this.executionMaxCount = builder.executionMaxCount;
    }

    @Override
    public boolean retryRequest(IOException exception, int executionCount, HttpContext context) {
        log.info("第" + executionCount + "次重试");

        if (executionCount >= this.executionMaxCount) {
            // Do not retry if over max retry count
            return false;
        }
        if (exception instanceof InterruptedIOException) {
            // Timeout
            return true;
        }
        if (exception instanceof UnknownHostException) {
            // Unknown host
            return true;
        }
        if (exception instanceof SSLException) {
            // SSL handshake exception
            return true;
        }
        if (exception instanceof NoHttpResponseException) {
            // No response
            return true;
        }

        HttpClientContext clientContext = HttpClientContext.adapt(context);
        HttpRequest request = clientContext.getRequest();
        boolean idempotent = !(request instanceof HttpEntityEnclosingRequest);
        // Retry if the request is considered idempotent
        return !idempotent;
    }

    public static final class Builder {
        private int executionMaxCount;

        public Builder() {
            executionMaxCount = 5;
        }

        public Builder executionCount(int executionCount) {
            this.executionMaxCount = executionCount;
            return this;
        }

        public HttpRequestRetryHandlerImpl build() {
            return new HttpRequestRetryHandlerImpl(this);
        }
    }
}
