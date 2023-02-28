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

package com.dtstack.chunjun.connector.selectdbcloud.rest.http;

import org.apache.flink.util.Preconditions;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

public class HttpPostBuilder {
    String url;
    Map<String, String> header;
    HttpEntity httpEntity;

    public HttpPostBuilder() {
        header = new HashMap<>();
    }

    public HttpPostBuilder setUrl(String url) {
        this.url = url;
        return this;
    }

    public HttpPostBuilder addCommonHeader() {
        header.put(HttpHeaders.EXPECT, "100-continue");
        return this;
    }

    public HttpPostBuilder baseAuth(String user, String password) {
        final String authInfo = user + ":" + password;
        header.put(
                HttpHeaders.AUTHORIZATION,
                "Basic "
                        + Base64.getEncoder()
                                .encodeToString(authInfo.getBytes(StandardCharsets.UTF_8)));
        return this;
    }

    public HttpPostBuilder setEntity(HttpEntity httpEntity) {
        this.httpEntity = httpEntity;
        return this;
    }

    public HttpPost build() {
        Preconditions.checkNotNull(url);
        Preconditions.checkNotNull(httpEntity);
        HttpPost put = new HttpPost(url);
        header.forEach(put::setHeader);
        put.setEntity(httpEntity);
        return put;
    }
}
